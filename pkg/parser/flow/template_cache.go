package flow

import (
	"fmt"
	"sync"
	"time"
)

// FieldSpec represents a field definition in a NetFlow v9 or IPFIX template.
type FieldSpec struct {
	TypeID           uint16
	Length           uint16
	EnterpriseNumber uint32 // Non-zero if enterprise bit was set (IPFIX)
}

// Template represents a registered template definition.
type Template struct {
	TemplateID uint16
	Fields     []FieldSpec
	LastSeen   time.Time
}

// TemplateCache is a thread-safe cache for flow templates.
type TemplateCache struct {
	mu        sync.RWMutex
	templates map[string]Template // key: "exporterIP:domainID:templateID"
	ttl       time.Duration
}

// NewTemplateCache creates a new template cache with the given TTL.
func NewTemplateCache(ttl time.Duration) *TemplateCache {
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	return &TemplateCache{
		templates: make(map[string]Template),
		ttl:       ttl,
	}
}

func cacheKey(exporterIP string, domainID uint32, templateID uint16) string {
	return fmt.Sprintf("%s:%d:%d", exporterIP, domainID, templateID)
}

// Get retrieves a template from the cache.
func (c *TemplateCache) Get(exporterIP string, domainID uint32, templateID uint16) (Template, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := cacheKey(exporterIP, domainID, templateID)
	tmpl, ok := c.templates[key]
	if !ok {
		return Template{}, false
	}
	if time.Since(tmpl.LastSeen) > c.ttl {
		return Template{}, false
	}
	return tmpl, true
}

// Set stores or updates a template in the cache.
func (c *TemplateCache) Set(exporterIP string, domainID uint32, templateID uint16, tmpl Template) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tmpl.LastSeen = time.Now()
	key := cacheKey(exporterIP, domainID, templateID)
	c.templates[key] = tmpl
}

// PruneStale removes templates that have not been seen within the TTL duration.
func (c *TemplateCache) PruneStale() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	now := time.Now()
	for k, tmpl := range c.templates {
		if now.Sub(tmpl.LastSeen) > c.ttl {
			delete(c.templates, k)
			count++
		}
	}
	return count
}
