package svc

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/SaiPisey2/cutover/internal/plan"
)

func deployPlan() (plan.Plan, plan.Service) {
	s := plan.Service{Name: "worker", Kind: plan.KindDeployment, Deployment: &plan.Deployment{Namespace: "shop", Name: "worker"}}
	return plan.Plan{Name: "shop", Services: []plan.Service{s}}, s
}

func kubeSide(c kubernetes.Interface) Side {
	return Side{Kube: func(context.Context) (kubernetes.Interface, error) { return c, nil }}
}

func meta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "shop", ResourceVersion: "123", UID: types.UID("u-" + name),
		Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": "{}"}}
}

func sourceObjects(extraVolumes ...corev1.Volume) []runtime.Object {
	replicas := int32(2)
	vols := append([]corev1.Volume{
		{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "worker-cfg"}}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}, extraVolumes...)
	dep := &appsv1.Deployment{ObjectMeta: meta("worker"), Spec: appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker", "tier": "bg"}},
			Spec: corev1.PodSpec{
				ServiceAccountName: "worker",
				ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "regcred"}},
				Volumes:            vols,
				Containers: []corev1.Container{{
					Name: "w", Image: "registry.example.com/shop/worker:2.0",
					EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "worker-env"}}}},
					Env: []corev1.EnvVar{{Name: "OPT", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "maybe"}, Key: "k", Optional: ptr(true)}}}},
				}},
			},
		},
	}}
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop"}},
		dep,
		&corev1.ConfigMap{ObjectMeta: meta("worker-cfg"), Data: map[string]string{"a": "b"}},
		&corev1.Secret{ObjectMeta: meta("worker-env"), Data: map[string][]byte{"STRIPE_KEY": []byte("SECRET-VALUE")}},
		&corev1.Secret{ObjectMeta: meta("regcred"), Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{".dockerconfigjson": []byte("{}")}},
		&corev1.ServiceAccount{ObjectMeta: meta("worker")},
		&corev1.Service{ObjectMeta: meta("worker"), Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "worker"}, ClusterIP: "10.96.0.7", ClusterIPs: []string{"10.96.0.7"},
			Type: corev1.ServiceTypeNodePort, Ports: []corev1.ServicePort{{Port: 80, NodePort: 30080}}}},
		&corev1.Service{ObjectMeta: meta("unrelated"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}}},
	}
}

func ptr[T any](v T) *T { return &v }

func TestDeploymentInspectClean(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	facts, err := d.Inspect(context.Background(), kubeSide(fake.NewClientset(sourceObjects()...)), kubeSide(fake.NewClientset()))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range facts.Findings {
		if f.Blocker {
			t.Fatalf("unexpected blocker %+v", f)
		}
	}
	if facts.Image != "registry.example.com/shop/worker:2.0" {
		t.Fatalf("image %q", facts.Image)
	}
}

func TestDeploymentFactsCarryNoSecretData(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	facts, _ := d.Inspect(context.Background(), kubeSide(fake.NewClientset(sourceObjects()...)), kubeSide(fake.NewClientset()))
	b, _ := json.Marshal(facts)
	if strings.Contains(string(b), "SECRET-VALUE") || strings.Contains(string(b), "STRIPE_KEY") {
		t.Fatalf("facts leak secret data: %s", b)
	}
}

func TestDeploymentInspectBlockers(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		vol  corev1.Volume
		code string
	}{
		{"pvc", corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}, "k8s-state"},
		{"hostpath", corev1.Volume{Name: "h", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/var/data"}}}, "k8s-state"},
		{"nfs", corev1.Volume{Name: "n", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "x", Path: "/"}}}, "k8s-volume"},
	} {
		facts, err := d.Inspect(ctx, kubeSide(fake.NewClientset(sourceObjects(tc.vol)...)), kubeSide(fake.NewClientset()))
		if err != nil || !codes(facts.Findings)[tc.code] {
			t.Errorf("%s: want %s, got %+v %v", tc.name, tc.code, facts.Findings, err)
		}
	}
	// A required secret missing on the source.
	objs := sourceObjects()
	var without []runtime.Object
	for _, o := range objs {
		if sec, ok := o.(*corev1.Secret); ok && sec.Name == "worker-env" {
			continue
		}
		without = append(without, o)
	}
	facts, _ := d.Inspect(ctx, kubeSide(fake.NewClientset(without...)), kubeSide(fake.NewClientset()))
	if !codes(facts.Findings)["k8s-ref-missing"] {
		t.Errorf("missing secret: %+v", facts.Findings)
	}
	// A same-named object on the target that cutover did not create.
	foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "worker-cfg", Namespace: "shop"}}
	facts, _ = d.Inspect(ctx, kubeSide(fake.NewClientset(sourceObjects()...)), kubeSide(fake.NewClientset(foreign)))
	if !codes(facts.Findings)["k8s-target-object"] {
		t.Errorf("foreign object: %+v", facts.Findings)
	}
	// No deployment at all.
	facts, _ = d.Inspect(ctx, kubeSide(fake.NewClientset()), kubeSide(fake.NewClientset()))
	if !codes(facts.Findings)["k8s-missing"] {
		t.Errorf("missing deployment: %+v", facts.Findings)
	}
}

func TestDeploymentPrepare(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	src, tgt := fake.NewClientset(sourceObjects()...), fake.NewClientset()
	if err := d.Prepare(ctx, kubeSide(src), kubeSide(tgt)); err != nil {
		t.Fatal(err)
	}
	ns, err := tgt.CoreV1().Namespaces().Get(ctx, "shop", metav1.GetOptions{})
	if err != nil || ns.Annotations[PlanAnnotation] != "shop" {
		t.Fatalf("namespace %+v %v", ns, err)
	}
	dep, err := tgt.AppsV1().Deployments("shop").Get(ctx, "worker", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if dep.Labels[ManagedByLabel] != ManagedBy || dep.Annotations[PlanAnnotation] != "shop" || dep.Annotations[ServiceAnnotation] != "worker" {
		t.Fatalf("ownership marks %+v %+v", dep.Labels, dep.Annotations)
	}
	if _, ok := dep.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok || dep.UID != "" {
		t.Fatal("source-only metadata copied")
	}
	sec, err := tgt.CoreV1().Secrets("shop").Get(ctx, "worker-env", metav1.GetOptions{})
	if err != nil || string(sec.Data["STRIPE_KEY"]) != "SECRET-VALUE" {
		t.Fatalf("secret not copied: %v", err)
	}
	svc, err := tgt.CoreV1().Services("shop").Get(ctx, "worker", metav1.GetOptions{})
	if err != nil || svc.Spec.ClusterIP != "" || len(svc.Spec.ClusterIPs) != 0 || svc.Spec.Ports[0].NodePort != 0 {
		t.Fatalf("service %+v %v", svc.Spec, err)
	}
	if _, err := tgt.CoreV1().Services("shop").Get(ctx, "unrelated", metav1.GetOptions{}); err == nil {
		t.Fatal("a service that does not select the deployment must not be copied")
	}
	if _, err := tgt.CoreV1().ConfigMaps("shop").Get(ctx, "maybe", metav1.GetOptions{}); err == nil {
		t.Fatal("an optional reference that does not exist is skipped")
	}
	// A retry is a no-op.
	if err := d.Prepare(ctx, kubeSide(src), kubeSide(tgt)); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestDeploymentPrepareRefusesBlocker(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	pvc := corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}
	err := d.Prepare(context.Background(), kubeSide(fake.NewClientset(sourceObjects(pvc)...)), kubeSide(fake.NewClientset()))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("got %v", err)
	}
}

func TestDeploymentHealthy(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	replicas := int32(2)
	mk := func(obs, gen int64, upd, avail int32) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "shop", Generation: gen},
			Spec:   appsv1.DeploymentSpec{Replicas: &replicas},
			Status: appsv1.DeploymentStatus{ObservedGeneration: obs, UpdatedReplicas: upd, AvailableReplicas: avail}}
	}
	for _, tc := range []struct {
		dep  *appsv1.Deployment
		want bool
	}{{mk(3, 3, 2, 2), true}, {mk(2, 3, 2, 2), false}, {mk(3, 3, 2, 1), false}} {
		ok, detail, err := d.Healthy(context.Background(), kubeSide(fake.NewClientset(tc.dep)))
		if err != nil || ok != tc.want || detail == "" {
			t.Errorf("%+v: %v %q %v", tc.dep.Status, ok, detail, err)
		}
	}
}

func TestDeploymentUndoKeepsForeignObjects(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	src := fake.NewClientset(sourceObjects()...)
	// The namespace already exists on the target (not cutover's), with a foreign object in it.
	tgt := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: "shop"}},
	)
	if err := d.Prepare(ctx, kubeSide(src), kubeSide(tgt)); err != nil {
		t.Fatal(err)
	}
	if err := d.Undo(ctx, kubeSide(tgt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.AppsV1().Deployments("shop").Get(ctx, "worker", metav1.GetOptions{}); err == nil {
		t.Fatal("cutover's deployment survived undo")
	}
	if _, err := tgt.CoreV1().Secrets("shop").Get(ctx, "worker-env", metav1.GetOptions{}); err == nil {
		t.Fatal("cutover's secret survived undo")
	}
	if _, err := tgt.CoreV1().ConfigMaps("shop").Get(ctx, "theirs", metav1.GetOptions{}); err != nil {
		t.Fatal("undo deleted an object cutover did not create")
	}
	if _, err := tgt.CoreV1().Namespaces().Get(ctx, "shop", metav1.GetOptions{}); err != nil {
		t.Fatal("undo deleted a namespace cutover did not create")
	}
}

func TestDeploymentUndoRemovesOwnNamespace(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	ctx := context.Background()
	tgt := fake.NewClientset()
	if err := d.Prepare(ctx, kubeSide(fake.NewClientset(sourceObjects()...)), kubeSide(tgt)); err != nil {
		t.Fatal(err)
	}
	if err := d.Undo(ctx, kubeSide(tgt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tgt.CoreV1().Namespaces().Get(ctx, "shop", metav1.GetOptions{}); err == nil {
		t.Fatal("cutover's own, now empty, namespace was left behind")
	}
}

func TestDeploymentFactsListContainerPorts(t *testing.T) {
	p, s := deployPlan()
	d, _ := For(p, s)
	objs := sourceObjects()
	for _, o := range objs {
		if dep, ok := o.(*appsv1.Deployment); ok {
			dep.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
				{Name: "http", ContainerPort: 8080},
				{ContainerPort: 9090, Protocol: corev1.ProtocolUDP},
				{ContainerPort: 2112},
				{ContainerPort: 2112, Protocol: corev1.ProtocolTCP},
			}
		}
	}
	facts, err := d.Inspect(context.Background(), kubeSide(fake.NewClientset(objs...)), kubeSide(fake.NewClientset()))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"2112/TCP", "9090/UDP", "http:8080/TCP"}
	if !slices.Equal(facts.Ports, want) {
		t.Fatalf("ports %v, want %v", facts.Ports, want)
	}
}
