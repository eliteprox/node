package orchestrator

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

const proxyMarker = "livepeer-proxy-id"

func validateProxyTemplate(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(strings.ReplaceAll(raw, "{proxy}", proxyMarker))
	if err != nil || strings.Count(raw, "{proxy}") != 1 || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("proxy-url-template requires one {proxy} and an HTTP(S) URL without credentials, query or fragment")
	}
	host := strings.SplitSeq(u.Hostname(), ".")
	for label := range host {
		if label == proxyMarker && u.Path == "" {
			return nil
		}
	}
	if !strings.Contains(u.Host, proxyMarker) && strings.HasSuffix(u.Path, "/"+proxyMarker) {
		return nil
	}
	return errors.New("{proxy} must be a complete hostname label (with no path) or the final path segment")
}

func (r *Registry) proxyURL(id string) string {
	if r.proxyTemplate != "" {
		return strings.Replace(r.proxyTemplate, "{proxy}", id, 1)
	}
	return r.service + "/run/" + id
}

// The template is fixed before serving. Match the actual Host, never a
// forwarded header supplied by a caller, and retain every application path.
func (r *Registry) matchProxy(req *http.Request) (string, string) {
	if r.proxyTemplate == "" {
		return "", ""
	}
	u, _ := url.Parse(strings.Replace(r.proxyTemplate, "{proxy}", proxyMarker, 1))
	if before, after, ok := strings.Cut(strings.ToLower(u.Host), proxyMarker); ok {
		host := strings.ToLower(req.Host)
		if strings.HasPrefix(host, before) && strings.HasSuffix(host, after) && len(host) > len(before)+len(after) {
			id := host[len(before) : len(host)-len(after)]
			if validRouteID(id) {
				return id, strings.TrimPrefix(req.URL.Path, "/")
			}
		}
	} else if strings.EqualFold(req.Host, u.Host) {
		prefix := strings.TrimSuffix(u.Path, proxyMarker)
		if tail, ok := strings.CutPrefix(req.URL.Path, prefix); ok {
			id, path, _ := strings.Cut(tail, "/")
			if validRouteID(id) {
				return id, path
			}
		}
	}
	return "", ""
}

func (r *Registry) discoveryURL(id string, item *app) string {
	if item.Proxy && item.Mode == "single-shot" {
		return r.proxyURL(id)
	}
	return r.appURL(id, item.Mode, "")
}

func (r *Registry) singleShotProxy(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	return item != nil && item.Proxy && item.Mode == "single-shot"
}

// Single-shot discovery uses the app ID as the proxy hostname label.
func (r *Registry) validateProxyApp(req appRequest) error {
	if !req.Proxy || req.Mode != "single-shot" || r.proxyTemplate == "" {
		return nil
	}
	u, _ := url.Parse(strings.Replace(r.proxyTemplate, "{proxy}", proxyMarker, 1))
	if !strings.Contains(u.Host, proxyMarker) {
		return nil
	}
	id := req.ID
	if len(id) > 63 || strings.HasPrefix(id, "-") || strings.HasSuffix(id, "-") || strings.ContainsAny(id, "_ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		return errors.New("single-shot domain proxy requires a lowercase DNS-label app ID of at most 63 characters")
	}
	return nil
}
