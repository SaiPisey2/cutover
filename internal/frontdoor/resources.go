package frontdoor

import (
	"fmt"
	"slices"
	"strings"
	"time"

	accesslogv3 "github.com/envoyproxy/go-control-plane/envoy/config/accesslog/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	filev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/access_loggers/file/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	reqwithoutqueryv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/formatter/req_without_query/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	ListenerName    = "frontdoor"
	RouteConfigName = "cutover"
)

// Listener is where Envoy accepts users' HTTP requests.
type Listener struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
	// AccessLog is the file Envoy writes failed requests to (see
	// accesslog.go). Empty means no access log.
	AccessLog string `json:"access_log,omitempty"`
}

// retryOn lists only failures where the request never reached the
// upstream, so a retry cannot run a write twice.
const retryOn = "connect-failure,refused-stream,reset-before-request"

// Build returns the Envoy resources for one snapshot: the route
// configuration comes from routes, the clusters from clusters. Staged
// changes (see xds.go) pass different sets; a route that references a
// cluster missing from clusters is an error, never a snapshot.
func Build(routes, clusters []Route, l Listener) (map[resource.Type][]types.Resource, error) {
	byName := map[string]*clusterv3.Cluster{}
	for _, r := range clusters {
		if r.Source != nil {
			n := ClusterName(r.Owner, r.Service, SideSource)
			byName[n] = cluster(n, *r.Source, r)
		}
		if r.Target != nil {
			n := ClusterName(r.Owner, r.Service, SideTarget)
			byName[n] = cluster(n, *r.Target, r)
		}
	}
	for _, r := range routes {
		need := r.ClusterNames()
		if r.Mirror {
			need = append(need, ClusterName(r.Owner, r.Service, SideTarget))
		}
		for _, n := range need {
			if byName[n] == nil {
				return nil, fmt.Errorf("route %s references cluster %s, which this snapshot lacks", r.Key(), n)
			}
		}
	}
	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	slices.Sort(names)
	var cls []types.Resource
	for _, n := range names {
		cls = append(cls, byName[n])
	}

	lis, err := listener(l)
	if err != nil {
		return nil, err
	}
	out := map[resource.Type][]types.Resource{
		resource.ClusterType:  cls,
		resource.RouteType:    {routeConfig(routes)},
		resource.ListenerType: {lis},
	}
	for _, rs := range out {
		for _, r := range rs {
			if v, ok := r.(interface{ Validate() error }); ok {
				if err := v.Validate(); err != nil {
					return nil, fmt.Errorf("invalid envoy resource: %w", err)
				}
			}
		}
	}
	return out, nil
}

func cluster(name string, u Upstream, r Route) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 name,
		ConnectTimeout:       durationpb.New(2 * time.Second),
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STRICT_DNS},
		DnsLookupFamily:      clusterv3.Cluster_V4_PREFERRED,
		LoadAssignment: &endpointv3.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*endpointv3.LocalityLbEndpoints{{
				LbEndpoints: []*endpointv3.LbEndpoint{{
					HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
						Address: socketAddress(u.Address, u.Port),
					}},
				}},
			}},
		},
		// The target receives no traffic before Shift; without a short
		// no_traffic_interval Envoy would check it once a minute and
		// Prepare's health wait would take minutes.
		HealthChecks: []*corev3.HealthCheck{{
			Timeout:            durationpb.New(time.Second),
			Interval:           durationpb.New(2 * time.Second),
			NoTrafficInterval:  durationpb.New(2 * time.Second),
			UnhealthyThreshold: wrapperspb.UInt32(3),
			HealthyThreshold:   wrapperspb.UInt32(2),
			HealthChecker: &corev3.HealthCheck_HttpHealthCheck_{HttpHealthCheck: &corev3.HealthCheck_HttpHealthCheck{
				Path: r.HealthPath,
				Host: r.Host,
			}},
		}},
	}
}

func socketAddress(addr string, port int) *corev3.Address {
	return &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
		Address:       addr,
		PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: uint32(port)},
	}}}
}

func listener(l Listener) (*listenerv3.Listener, error) {
	router, err := anypb.New(&routerv3.Router{})
	if err != nil {
		return nil, err
	}
	var logs []*accesslogv3.AccessLog
	if l.AccessLog != "" {
		al, err := failureLog(l.AccessLog)
		if err != nil {
			return nil, err
		}
		logs = append(logs, al)
	}
	hcm, err := anypb.New(&hcmv3.HttpConnectionManager{
		StatPrefix: "frontdoor",
		AccessLog:  logs,
		CodecType:  hcmv3.HttpConnectionManager_AUTO,
		RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{Rds: &hcmv3.Rds{
			RouteConfigName: RouteConfigName,
			ConfigSource: &corev3.ConfigSource{
				ResourceApiVersion:    corev3.ApiVersion_V3,
				ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
			},
		}},
		HttpFilters: []*hcmv3.HttpFilter{{
			Name:       "envoy.filters.http.router",
			ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: router},
		}},
	})
	if err != nil {
		return nil, err
	}
	return &listenerv3.Listener{
		Name:    ListenerName,
		Address: socketAddress(l.Address, l.Port),
		FilterChains: []*listenerv3.FilterChain{{
			Filters: []*listenerv3.Filter{{
				Name:       "envoy.filters.network.http_connection_manager",
				ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: hcm},
			}},
		}},
	}, nil
}

// failureFormat is one JSON object per failed request. It names no
// header but the request ID and no body, and the path drops the query
// (which can carry tokens) and is cut at 200 bytes.
var failureFormat = map[string]any{
	"at":          "%START_TIME%",
	"method":      "%REQ(:METHOD)%",
	"path":        "%REQ_WITHOUT_QUERY(:PATH):200%",
	"status":      "%RESPONSE_CODE%",
	"flags":       "%RESPONSE_FLAGS%",
	"duration_ms": "%DURATION%",
	"cluster":     "%UPSTREAM_CLUSTER%",
	"request_id":  "%REQ(X-REQUEST-ID)%",
}

// failureLog writes requests that got a 5xx, or that Envoy flagged
// (no healthy upstream, reset, timeout, ...), to path.
func failureLog(path string) (*accesslogv3.AccessLog, error) {
	format, err := structpb.NewStruct(failureFormat)
	if err != nil {
		return nil, err
	}
	reqWithoutQuery, err := anypb.New(&reqwithoutqueryv3.ReqWithoutQuery{})
	if err != nil {
		return nil, err
	}
	file := &filev3.FileAccessLog{
		Path: path,
		AccessLogFormat: &filev3.FileAccessLog_LogFormat{LogFormat: &corev3.SubstitutionFormatString{
			Format: &corev3.SubstitutionFormatString_JsonFormat{JsonFormat: format},
			Formatters: []*corev3.TypedExtensionConfig{{
				Name:        "envoy.formatter.req_without_query",
				TypedConfig: reqWithoutQuery,
			}},
		}},
	}
	if err := file.Validate(); err != nil {
		return nil, fmt.Errorf("invalid access log: %w", err)
	}
	typed, err := anypb.New(file)
	if err != nil {
		return nil, err
	}
	al := &accesslogv3.AccessLog{
		Name: "envoy.access_loggers.file",
		Filter: &accesslogv3.AccessLogFilter{FilterSpecifier: &accesslogv3.AccessLogFilter_OrFilter{OrFilter: &accesslogv3.OrFilter{
			Filters: []*accesslogv3.AccessLogFilter{
				{FilterSpecifier: &accesslogv3.AccessLogFilter_StatusCodeFilter{StatusCodeFilter: &accesslogv3.StatusCodeFilter{
					Comparison: &accesslogv3.ComparisonFilter{
						Op:    accesslogv3.ComparisonFilter_GE,
						Value: &corev3.RuntimeUInt32{DefaultValue: 500, RuntimeKey: "cutover.access_log.min_status"},
					},
				}}},
				// No flags listed: any response flag matches.
				{FilterSpecifier: &accesslogv3.AccessLogFilter_ResponseFlagFilter{ResponseFlagFilter: &accesslogv3.ResponseFlagFilter{}}},
			},
		}}},
		ConfigType: &accesslogv3.AccessLog_TypedConfig{TypedConfig: typed},
	}
	if err := al.Validate(); err != nil {
		return nil, fmt.Errorf("invalid access log: %w", err)
	}
	return al, nil
}

func routeConfig(routes []Route) *routev3.RouteConfiguration {
	byHost := map[string][]Route{}
	for _, r := range routes {
		byHost[r.Host] = append(byHost[r.Host], r)
	}
	hosts := make([]string, 0, len(byHost))
	for h := range byHost {
		hosts = append(hosts, h)
	}
	slices.Sort(hosts)
	rc := &routev3.RouteConfiguration{Name: RouteConfigName}
	for _, h := range hosts {
		rs := byHost[h]
		// Longest prefix first: Envoy takes the first route that matches.
		slices.SortFunc(rs, func(a, b Route) int {
			if len(a.Prefix) != len(b.Prefix) {
				return len(b.Prefix) - len(a.Prefix)
			}
			return strings.Compare(a.Prefix, b.Prefix)
		})
		vh := &routev3.VirtualHost{Name: h, Domains: []string{h, h + ":*"}}
		for _, r := range rs {
			if r.Mirror {
				methods := slices.Clone(r.MirrorMethods)
				slices.Sort(methods)
				mirror := action(r)
				mirror.RequestMirrorPolicies = []*routev3.RouteAction_RequestMirrorPolicy{{
					Cluster: ClusterName(r.Owner, r.Service, SideTarget),
					RuntimeFraction: &corev3.RuntimeFractionalPercent{
						DefaultValue: &typev3.FractionalPercent{Numerator: 100, Denominator: typev3.FractionalPercent_HUNDRED},
					},
					// The target sees the Host users sent, not "<host>-shadow".
					DisableShadowHostSuffixAppend: true,
				}}
				vh.Routes = append(vh.Routes, &routev3.Route{
					Name: r.Key() + "/mirror",
					Match: &routev3.RouteMatch{
						PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: r.Prefix},
						Headers: []*routev3.HeaderMatcher{{
							Name: ":method",
							HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{StringMatch: &matcherv3.StringMatcher{
								MatchPattern: &matcherv3.StringMatcher_SafeRegex{SafeRegex: &matcherv3.RegexMatcher{
									Regex: "^(" + strings.Join(methods, "|") + ")$",
								}},
							}},
						}},
					},
					Action: &routev3.Route_Route{Route: mirror},
				})
			}
			vh.Routes = append(vh.Routes, &routev3.Route{
				Name:   r.Key(),
				Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: r.Prefix}},
				Action: &routev3.Route_Route{Route: action(r)},
			})
		}
		rc.VirtualHosts = append(rc.VirtualHosts, vh)
	}
	return rc
}

// action picks the cluster(s) for r. A weight of 0 or 100 routes straight
// to one cluster; anything between splits by weighted clusters.
func action(r Route) *routev3.RouteAction {
	src := ClusterName(r.Owner, r.Service, SideSource)
	tgt := ClusterName(r.Owner, r.Service, SideTarget)
	a := &routev3.RouteAction{RetryPolicy: &routev3.RetryPolicy{RetryOn: retryOn, NumRetries: wrapperspb.UInt32(2)}}
	switch {
	case r.Source == nil || (r.Target != nil && r.TargetWeight == 100):
		a.ClusterSpecifier = &routev3.RouteAction_Cluster{Cluster: tgt}
	case r.Target == nil || r.TargetWeight == 0:
		a.ClusterSpecifier = &routev3.RouteAction_Cluster{Cluster: src}
	default:
		a.ClusterSpecifier = &routev3.RouteAction_WeightedClusters{WeightedClusters: &routev3.WeightedCluster{
			Clusters: []*routev3.WeightedCluster_ClusterWeight{
				{Name: src, Weight: wrapperspb.UInt32(uint32(100 - r.TargetWeight))},
				{Name: tgt, Weight: wrapperspb.UInt32(uint32(r.TargetWeight))},
			},
		}}
	}
	return proto.Clone(a).(*routev3.RouteAction)
}
