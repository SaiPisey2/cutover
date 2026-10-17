package svc

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/SaiPisey2/cutover/internal/plan"
)

const (
	ManagedByLabel    = "app.kubernetes.io/managed-by"
	ManagedBy         = "cutover"
	PlanAnnotation    = "cutover.dev/plan"
	ServiceAnnotation = "cutover.dev/service"
)

type deployment struct {
	plan string
	svc  plan.Service
}

func (d deployment) ns() string   { return d.svc.Deployment.Namespace }
func (d deployment) name() string { return d.svc.Deployment.Name }

// refs are the objects the deployment needs copied alongside it.
type refs struct {
	configMaps, secrets []string
	optional            map[string]bool // "ConfigMap/x" or "Secret/x"
	serviceAccount      string
	services            []string
}

func addRef(list *[]string, name string) {
	if name != "" && !slices.Contains(*list, name) {
		*list = append(*list, name)
	}
}

func (d deployment) collect(ctx context.Context, c kubernetes.Interface, dep *appsv1.Deployment) (refs, []Finding, error) {
	r := refs{optional: map[string]bool{}}
	var fs []Finding
	pod := dep.Spec.Template.Spec
	subj := d.ns() + "/" + d.name()
	opt := func(kind, name string, o *bool) {
		if o != nil && *o {
			r.optional[kind+"/"+name] = true
		}
	}
	for _, v := range pod.Volumes {
		switch {
		case v.ConfigMap != nil:
			addRef(&r.configMaps, v.ConfigMap.Name)
			opt("ConfigMap", v.ConfigMap.Name, v.ConfigMap.Optional)
		case v.Secret != nil:
			addRef(&r.secrets, v.Secret.SecretName)
			opt("Secret", v.Secret.SecretName, v.Secret.Optional)
		case v.Projected != nil:
			for _, src := range v.Projected.Sources {
				if src.ConfigMap != nil {
					addRef(&r.configMaps, src.ConfigMap.Name)
					opt("ConfigMap", src.ConfigMap.Name, src.ConfigMap.Optional)
				}
				if src.Secret != nil {
					addRef(&r.secrets, src.Secret.Name)
					opt("Secret", src.Secret.Name, src.Secret.Optional)
				}
			}
		case v.EmptyDir != nil, v.DownwardAPI != nil:
		case v.PersistentVolumeClaim != nil:
			fs = append(fs, Finding{Blocker: true, Code: "k8s-state", Subject: subj,
				Message: fmt.Sprintf("%s mounts persistent volume claim %s: that data stays in the source cluster", subj, v.PersistentVolumeClaim.ClaimName)})
		case v.HostPath != nil:
			fs = append(fs, Finding{Blocker: true, Code: "k8s-state", Subject: subj,
				Message: fmt.Sprintf("%s mounts host path %s: that data is on a source node's disk", subj, v.HostPath.Path)})
		default:
			fs = append(fs, Finding{Blocker: true, Code: "k8s-volume", Subject: subj,
				Message: fmt.Sprintf("%s uses volume %s of a type cutover does not copy (emptyDir, configMap, secret, projected and downwardAPI only)", subj, v.Name)})
		}
	}
	for _, ct := range slices.Concat(pod.InitContainers, pod.Containers) {
		for _, e := range ct.Env {
			if e.ValueFrom == nil {
				continue
			}
			if k := e.ValueFrom.ConfigMapKeyRef; k != nil {
				addRef(&r.configMaps, k.Name)
				opt("ConfigMap", k.Name, k.Optional)
			}
			if k := e.ValueFrom.SecretKeyRef; k != nil {
				addRef(&r.secrets, k.Name)
				opt("Secret", k.Name, k.Optional)
			}
		}
		for _, e := range ct.EnvFrom {
			if e.ConfigMapRef != nil {
				addRef(&r.configMaps, e.ConfigMapRef.Name)
				opt("ConfigMap", e.ConfigMapRef.Name, e.ConfigMapRef.Optional)
			}
			if e.SecretRef != nil {
				addRef(&r.secrets, e.SecretRef.Name)
				opt("Secret", e.SecretRef.Name, e.SecretRef.Optional)
			}
		}
	}
	for _, ps := range pod.ImagePullSecrets {
		addRef(&r.secrets, ps.Name)
	}
	if sa := pod.ServiceAccountName; sa != "" && sa != "default" {
		r.serviceAccount = sa
	}
	svcs, err := c.CoreV1().Services(d.ns()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return r, nil, err
	}
	podLabels := labels.Set(dep.Spec.Template.Labels)
	for _, s := range svcs.Items {
		if len(s.Spec.Selector) > 0 && labels.SelectorFromSet(s.Spec.Selector).Matches(podLabels) {
			r.services = append(r.services, s.Name)
		}
	}
	slices.Sort(r.configMaps)
	slices.Sort(r.secrets)
	slices.Sort(r.services)
	return r, fs, nil
}

// ownedBy reports whether an object carries this plan's ownership marks.
func (d deployment) ownedBy(m metav1.ObjectMeta) bool {
	return m.Labels[ManagedByLabel] == ManagedBy && m.Annotations[PlanAnnotation] == d.plan
}

// work is everything Inspect found: findings plus what Prepare copies.
type work struct {
	findings []Finding
	refs     refs
	skip     map[string]bool // "Kind/name" not to copy (absent optional refs, SA tokens)
	image    string
	ports    []string
}

func (d deployment) inspect(ctx context.Context, src, tgt Side) (work, error) {
	w := work{skip: map[string]bool{}}
	sc, err := src.Kube(ctx)
	if err != nil {
		return w, fmt.Errorf("source: %w", err)
	}
	tc, err := tgt.Kube(ctx)
	if err != nil {
		return w, fmt.Errorf("target: %w", err)
	}
	subj := d.ns() + "/" + d.name()
	dep, err := sc.AppsV1().Deployments(d.ns()).Get(ctx, d.name(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		w.findings = append(w.findings, Finding{Blocker: true, Code: "k8s-missing", Subject: subj, Message: "deployment " + subj + " is not in the source cluster"})
		return w, nil
	}
	if err != nil {
		return w, fmt.Errorf("source: %w", err)
	}
	if cs := dep.Spec.Template.Spec.Containers; len(cs) > 0 {
		w.image = cs[0].Image
	}
	w.ports = containerPorts(dep.Spec.Template.Spec.Containers)
	r, fs, err := d.collect(ctx, sc, dep)
	if err != nil {
		return w, fmt.Errorf("source: %w", err)
	}
	w.refs, w.findings = r, fs
	add := func(blocker bool, code, subject, msg string) {
		w.findings = append(w.findings, Finding{Blocker: blocker, Code: code, Subject: subject, Message: msg})
	}
	// Every referenced object must exist on the source (unless optional).
	for _, n := range r.configMaps {
		if _, err := sc.CoreV1().ConfigMaps(d.ns()).Get(ctx, n, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			if r.optional["ConfigMap/"+n] {
				w.skip["ConfigMap/"+n] = true
				continue
			}
			add(true, "k8s-ref-missing", "ConfigMap/"+n, fmt.Sprintf("%s needs ConfigMap %s, which is not in the source cluster", subj, n))
		} else if err != nil {
			return w, fmt.Errorf("source: %w", err)
		}
	}
	for _, n := range r.secrets {
		s, err := sc.CoreV1().Secrets(d.ns()).Get(ctx, n, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			if r.optional["Secret/"+n] {
				w.skip["Secret/"+n] = true
				continue
			}
			add(true, "k8s-ref-missing", "Secret/"+n, fmt.Sprintf("%s needs Secret %s, which is not in the source cluster", subj, n))
		case err != nil:
			return w, fmt.Errorf("source: %w", err)
		case s.Type == corev1.SecretTypeServiceAccountToken:
			w.skip["Secret/"+n] = true
			add(false, "k8s-sa-token", "Secret/"+n, fmt.Sprintf("Secret %s is a service-account token; the target cluster issues its own", n))
		}
	}
	if sa := r.serviceAccount; sa != "" {
		if _, err := sc.CoreV1().ServiceAccounts(d.ns()).Get(ctx, sa, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			add(true, "k8s-ref-missing", "ServiceAccount/"+sa, fmt.Sprintf("%s runs as ServiceAccount %s, which is not in the source cluster", subj, sa))
		} else if err != nil {
			return w, fmt.Errorf("source: %w", err)
		}
	}
	// Nothing cutover would create may already exist on the target
	// unless this plan created it.
	check := func(kind, name string, get func() (metav1.ObjectMeta, error)) error {
		if w.skip[kind+"/"+name] {
			return nil
		}
		m, err := get()
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("target: %w", err)
		}
		if !d.ownedBy(m) {
			add(true, "k8s-target-object", kind+"/"+name, fmt.Sprintf("%s %s/%s already exists in the target cluster; cutover never replaces an object it did not create", kind, d.ns(), name))
		}
		return nil
	}
	ctc := tc.CoreV1()
	for _, n := range r.configMaps {
		if err := check("ConfigMap", n, func() (metav1.ObjectMeta, error) {
			o, err := ctc.ConfigMaps(d.ns()).Get(ctx, n, metav1.GetOptions{})
			return metaOf(o, err)
		}); err != nil {
			return w, err
		}
	}
	for _, n := range r.secrets {
		if err := check("Secret", n, func() (metav1.ObjectMeta, error) {
			o, err := ctc.Secrets(d.ns()).Get(ctx, n, metav1.GetOptions{})
			return metaOf(o, err)
		}); err != nil {
			return w, err
		}
	}
	if sa := r.serviceAccount; sa != "" {
		if err := check("ServiceAccount", sa, func() (metav1.ObjectMeta, error) {
			o, err := ctc.ServiceAccounts(d.ns()).Get(ctx, sa, metav1.GetOptions{})
			return metaOf(o, err)
		}); err != nil {
			return w, err
		}
	}
	for _, n := range r.services {
		if err := check("Service", n, func() (metav1.ObjectMeta, error) {
			o, err := ctc.Services(d.ns()).Get(ctx, n, metav1.GetOptions{})
			return metaOf(o, err)
		}); err != nil {
			return w, err
		}
	}
	if err := check("Deployment", d.name(), func() (metav1.ObjectMeta, error) {
		o, err := tc.AppsV1().Deployments(d.ns()).Get(ctx, d.name(), metav1.GetOptions{})
		return metaOf(o, err)
	}); err != nil {
		return w, err
	}
	return w, nil
}

// metaOf returns an object's metadata through any typed Get's result.
func metaOf[T interface{ GetObjectMeta() metav1.Object }](o T, err error) (metav1.ObjectMeta, error) {
	if err != nil {
		return metav1.ObjectMeta{}, err
	}
	m := o.GetObjectMeta()
	return metav1.ObjectMeta{Name: m.GetName(), Labels: m.GetLabels(), Annotations: m.GetAnnotations()}, nil
}

func (d deployment) Inspect(ctx context.Context, src, tgt Side) (Facts, error) {
	w, err := d.inspect(ctx, src, tgt)
	return Facts{Service: d.svc.Name, Kind: plan.KindDeployment, Image: w.image, Ports: w.ports, Findings: w.findings}, err
}

// containerPorts lists the containers' ports as "name:port/proto" (or
// "port/proto" when unnamed), sorted and without duplicates.
func containerPorts(cs []corev1.Container) []string {
	var out []string
	for _, c := range cs {
		for _, p := range c.Ports {
			proto := string(p.Protocol)
			if proto == "" {
				proto = string(corev1.ProtocolTCP)
			}
			s := fmt.Sprintf("%d/%s", p.ContainerPort, proto)
			if p.Name != "" {
				s = p.Name + ":" + s
			}
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// clean keeps what a copy needs and marks it as cutover's.
func (d deployment) clean(m metav1.ObjectMeta) metav1.ObjectMeta {
	out := metav1.ObjectMeta{Name: m.Name, Namespace: m.Namespace, Labels: maps.Clone(m.Labels), Annotations: maps.Clone(m.Annotations)}
	if out.Labels == nil {
		out.Labels = map[string]string{}
	}
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	delete(out.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
	delete(out.Annotations, "deployment.kubernetes.io/revision")
	out.Labels[ManagedByLabel] = ManagedBy
	out.Annotations[PlanAnnotation] = d.plan
	out.Annotations[ServiceAnnotation] = d.svc.Name
	return out
}

// create makes obj on the target. An object already there is fine if
// this plan made it (a retry); otherwise Prepare stops.
func (d deployment) create(kind, name string, do func() error, get func() (metav1.ObjectMeta, error)) error {
	err := do()
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("target: create %s %s/%s: %w", kind, d.ns(), name, err)
	}
	m, gerr := get()
	if gerr != nil {
		return fmt.Errorf("target: %w", gerr)
	}
	if !d.ownedBy(m) {
		return fmt.Errorf("%w: %s %s/%s appeared in the target cluster and is not cutover's", ErrBlocked, kind, d.ns(), name)
	}
	return nil
}

func (d deployment) Prepare(ctx context.Context, src, tgt Side) error {
	w, err := d.inspect(ctx, src, tgt)
	if err != nil {
		return err
	}
	if err := blocked(w.findings); err != nil {
		return err
	}
	sc, _ := src.Kube(ctx)
	tc, _ := tgt.Kube(ctx)
	ns := d.ns()
	if _, err := tc.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		n := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns,
			Labels:      map[string]string{ManagedByLabel: ManagedBy},
			Annotations: map[string]string{PlanAnnotation: d.plan, ServiceAnnotation: d.svc.Name}}}
		if _, err := tc.CoreV1().Namespaces().Create(ctx, n, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("target: create namespace %s: %w", ns, err)
		}
	} else if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	core := tc.CoreV1()
	for _, n := range w.refs.configMaps {
		if w.skip["ConfigMap/"+n] {
			continue
		}
		o, err := sc.CoreV1().ConfigMaps(ns).Get(ctx, n, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		cp := &corev1.ConfigMap{ObjectMeta: d.clean(o.ObjectMeta), Data: o.Data, BinaryData: o.BinaryData, Immutable: o.Immutable}
		if err := d.create("ConfigMap", n, func() error { _, err := core.ConfigMaps(ns).Create(ctx, cp, metav1.CreateOptions{}); return err },
			func() (metav1.ObjectMeta, error) {
				o, err := core.ConfigMaps(ns).Get(ctx, n, metav1.GetOptions{})
				return metaOf(o, err)
			}); err != nil {
			return err
		}
	}
	for _, n := range w.refs.secrets {
		if w.skip["Secret/"+n] {
			continue
		}
		o, err := sc.CoreV1().Secrets(ns).Get(ctx, n, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		cp := &corev1.Secret{ObjectMeta: d.clean(o.ObjectMeta), Type: o.Type, Data: o.Data, Immutable: o.Immutable}
		if err := d.create("Secret", n, func() error { _, err := core.Secrets(ns).Create(ctx, cp, metav1.CreateOptions{}); return err },
			func() (metav1.ObjectMeta, error) {
				o, err := core.Secrets(ns).Get(ctx, n, metav1.GetOptions{})
				return metaOf(o, err)
			}); err != nil {
			return err
		}
	}
	if sa := w.refs.serviceAccount; sa != "" {
		o, err := sc.CoreV1().ServiceAccounts(ns).Get(ctx, sa, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		cp := &corev1.ServiceAccount{ObjectMeta: d.clean(o.ObjectMeta), ImagePullSecrets: o.ImagePullSecrets, AutomountServiceAccountToken: o.AutomountServiceAccountToken}
		if err := d.create("ServiceAccount", sa, func() error { _, err := core.ServiceAccounts(ns).Create(ctx, cp, metav1.CreateOptions{}); return err },
			func() (metav1.ObjectMeta, error) {
				o, err := core.ServiceAccounts(ns).Get(ctx, sa, metav1.GetOptions{})
				return metaOf(o, err)
			}); err != nil {
			return err
		}
	}
	for _, n := range w.refs.services {
		o, err := sc.CoreV1().Services(ns).Get(ctx, n, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		spec := *o.Spec.DeepCopy()
		spec.ClusterIP, spec.ClusterIPs, spec.HealthCheckNodePort = "", nil, 0
		for i := range spec.Ports {
			spec.Ports[i].NodePort = 0
		}
		cp := &corev1.Service{ObjectMeta: d.clean(o.ObjectMeta), Spec: spec}
		if err := d.create("Service", n, func() error { _, err := core.Services(ns).Create(ctx, cp, metav1.CreateOptions{}); return err },
			func() (metav1.ObjectMeta, error) {
				o, err := core.Services(ns).Get(ctx, n, metav1.GetOptions{})
				return metaOf(o, err)
			}); err != nil {
			return err
		}
	}
	dep, err := sc.AppsV1().Deployments(ns).Get(ctx, d.name(), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	cp := &appsv1.Deployment{ObjectMeta: d.clean(dep.ObjectMeta), Spec: *dep.Spec.DeepCopy()}
	apps := tc.AppsV1()
	return d.create("Deployment", d.name(), func() error { _, err := apps.Deployments(ns).Create(ctx, cp, metav1.CreateOptions{}); return err },
		func() (metav1.ObjectMeta, error) {
			o, err := apps.Deployments(ns).Get(ctx, d.name(), metav1.GetOptions{})
			return metaOf(o, err)
		})
}

func (d deployment) Healthy(ctx context.Context, tgt Side) (bool, string, error) {
	c, err := tgt.Kube(ctx)
	if err != nil {
		return false, "", err
	}
	dep, err := c.AppsV1().Deployments(d.ns()).Get(ctx, d.name(), metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	want := int32(1)
	if dep.Spec.Replicas != nil {
		want = *dep.Spec.Replicas
	}
	st := dep.Status
	ok := st.ObservedGeneration >= dep.Generation && st.UpdatedReplicas == want && st.AvailableReplicas == want
	return ok, fmt.Sprintf("%s/%s: %d of %d available, %d updated", d.ns(), d.name(), st.AvailableReplicas, want, st.UpdatedReplicas), nil
}

// mine selects this plan's and this service's objects.
func (d deployment) mine(m metav1.Object) bool {
	return m.GetLabels()[ManagedByLabel] == ManagedBy && m.GetAnnotations()[PlanAnnotation] == d.plan && m.GetAnnotations()[ServiceAnnotation] == d.svc.Name
}

func (d deployment) Undo(ctx context.Context, tgt Side) error {
	c, err := tgt.Kube(ctx)
	if err != nil {
		return err
	}
	ns := d.ns()
	sel := metav1.ListOptions{LabelSelector: ManagedByLabel + "=" + ManagedBy}
	del := func(kind, name string, fn func() error) error {
		if err := fn(); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %s %s/%s: %w", kind, ns, name, err)
		}
		return nil
	}
	deps, err := c.AppsV1().Deployments(ns).List(ctx, sel)
	if err != nil {
		return err
	}
	for _, o := range deps.Items {
		if d.mine(&o) {
			if err := del("Deployment", o.Name, func() error { return c.AppsV1().Deployments(ns).Delete(ctx, o.Name, metav1.DeleteOptions{}) }); err != nil {
				return err
			}
		}
	}
	svcs, err := c.CoreV1().Services(ns).List(ctx, sel)
	if err != nil {
		return err
	}
	for _, o := range svcs.Items {
		if d.mine(&o) {
			if err := del("Service", o.Name, func() error { return c.CoreV1().Services(ns).Delete(ctx, o.Name, metav1.DeleteOptions{}) }); err != nil {
				return err
			}
		}
	}
	sas, err := c.CoreV1().ServiceAccounts(ns).List(ctx, sel)
	if err != nil {
		return err
	}
	for _, o := range sas.Items {
		if d.mine(&o) {
			if err := del("ServiceAccount", o.Name, func() error { return c.CoreV1().ServiceAccounts(ns).Delete(ctx, o.Name, metav1.DeleteOptions{}) }); err != nil {
				return err
			}
		}
	}
	secs, err := c.CoreV1().Secrets(ns).List(ctx, sel)
	if err != nil {
		return err
	}
	for _, o := range secs.Items {
		if d.mine(&o) {
			if err := del("Secret", o.Name, func() error { return c.CoreV1().Secrets(ns).Delete(ctx, o.Name, metav1.DeleteOptions{}) }); err != nil {
				return err
			}
		}
	}
	cms, err := c.CoreV1().ConfigMaps(ns).List(ctx, sel)
	if err != nil {
		return err
	}
	for _, o := range cms.Items {
		if d.mine(&o) {
			if err := del("ConfigMap", o.Name, func() error { return c.CoreV1().ConfigMaps(ns).Delete(ctx, o.Name, metav1.DeleteOptions{}) }); err != nil {
				return err
			}
		}
	}
	n, err := c.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !d.mine(n) {
		return nil
	}
	empty, err := d.emptyNamespace(ctx, c)
	if err != nil || !empty {
		return err
	}
	return del("Namespace", ns, func() error { return c.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}) })
}

// emptyNamespace reports whether the namespace holds nothing a person
// put there: no deployments or services, no config maps but the
// cluster's own root CA, no secrets but service-account tokens.
func (d deployment) emptyNamespace(ctx context.Context, c kubernetes.Interface) (bool, error) {
	ns := d.ns()
	deps, err := c.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if err != nil || len(deps.Items) > 0 {
		return false, err
	}
	svcs, err := c.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
	if err != nil || len(svcs.Items) > 0 {
		return false, err
	}
	cms, err := c.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, o := range cms.Items {
		if o.Name != "kube-root-ca.crt" {
			return false, nil
		}
	}
	secs, err := c.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, o := range secs.Items {
		if o.Type != corev1.SecretTypeServiceAccountToken {
			return false, nil
		}
	}
	return !strings.HasPrefix(ns, "kube-"), nil
}
