package k8sclient

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const DefaultLabelPrefix = "convex"

const (
	priorityLeaderDefault   = 100
	priorityFollowerDefault = 0
)

const BackendPort = 3210

type Backend struct {
	Pod      string
	URL      string
	Priority int
}

type labelKeys struct {
	deployment     string
	role           string
	component      string
	leaderPriority string
}

func newLabelKeys(prefix string) labelKeys {
	if prefix == "" {
		prefix = DefaultLabelPrefix
	}
	return labelKeys{
		deployment:     prefix + "/instance",
		role:           prefix + "/role",
		component:      prefix + "/component",
		leaderPriority: prefix + "/leader-priority",
	}
}

type Client struct {
	cs     kubernetes.Interface
	labels labelKeys
}

func New(kubeconfigPath, labelPrefix string) (*Client, error) {
	cfg, err := loadConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return &Client{cs: cs, labels: newLabelKeys(labelPrefix)}, nil
}

func NewFromInterface(cs kubernetes.Interface, labelPrefix string) *Client {
	return &Client{cs: cs, labels: newLabelKeys(labelPrefix)}
}

func (c *Client) Clientset() kubernetes.Interface {
	return c.cs
}

func loadConfig(kubeconfigPath string) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	} else if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	return cfg, nil
}

func (c *Client) DiscoverBackends(ctx context.Context, ns, name string) ([]Backend, error) {
	sel := fmt.Sprintf("%s=%s,%s,%s=backend", c.labels.deployment, name, c.labels.role, c.labels.component)
	list, err := c.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, fmt.Errorf("list pods (%s in %s): %w", sel, ns, err)
	}
	out := make([]Backend, 0, len(list.Items))
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil || p.Status.PodIP == "" {
			continue
		}
		if p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
			continue
		}
		role := p.Labels[c.labels.role]
		out = append(out, Backend{
			Pod:      p.Name,
			URL:      fmt.Sprintf("http://%s", net.JoinHostPort(p.Status.PodIP, strconv.Itoa(BackendPort))),
			Priority: priorityFor(p.Labels[c.labels.leaderPriority], role),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pod < out[j].Pod })
	return out, nil
}

func priorityFor(label, role string) int {
	if label != "" {
		if n, err := strconv.Atoi(label); err == nil {
			return n
		}
	}
	if role == "leader" {
		return priorityLeaderDefault
	}
	return priorityFollowerDefault
}

func PodReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
