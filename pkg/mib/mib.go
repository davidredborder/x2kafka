package mib

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/golangsnmp/gomib"
	"github.com/golangsnmp/gomib/mib"
)

var (
	MibEngine    *mib.Mib
	oidToNameMap = make(map[string]string)
	MibEngineMu  sync.RWMutex
)

// InitMIBs initializes the dynamic MIB parser engine with paths from mib_dirs config.
func InitMIBs(dirs []string) error {
	MibEngineMu.Lock()
	defer MibEngineMu.Unlock()

	var sources []gomib.Source
	for _, d := range dirs {
		if d == "" {
			continue
		}
		if _, err := os.Stat(d); os.IsNotExist(err) {
			log.Printf("[MIB Warning] MIB directory %s does not exist", d)
			continue
		}
		src, err := gomib.Dir(d)
		if err != nil {
			log.Printf("[MIB Error] Failed to parse MIB directory %s: %v", d, err)
			continue
		}
		sources = append(sources, src)
	}

	var m *mib.Mib
	var err error

	cfg := mib.DefaultConfig()
	cfg.FailAt = mib.SeverityFatal

	if len(sources) == 0 {
		m, err = gomib.Load(context.Background(), gomib.WithSystemPaths(), gomib.WithDiagnosticConfig(cfg))
		if err != nil {
			log.Printf("[MIB Info] No MIB directories configured, system MIB auto-discovery deferred.")
			MibEngine = nil
			rebuildOidMap(nil)
			return nil
		}
		MibEngine = m
		rebuildOidMap(m)
		log.Printf("[MIB Info] Auto-discovered system MIBs successfully")
		return nil
	}

	var finalSource gomib.Source
	if len(sources) == 1 {
		finalSource = sources[0]
	} else {
		finalSource = gomib.Multi(sources...)
	}

	m, err = gomib.Load(context.Background(), gomib.WithSource(finalSource), gomib.WithDiagnosticConfig(cfg))
	if err != nil {
		log.Printf("[MIB Error] Failed to load MIB files: %v", err)
		MibEngine = nil
		rebuildOidMap(nil)
		return err
	}

	MibEngine = m
	rebuildOidMap(m)
	log.Printf("[MIB Info] Successfully loaded custom MIB files from %v", dirs)
	return nil
}

func rebuildOidMap(m *mib.Mib) {
	newMap := make(map[string]string)
	if m == nil {
		oidToNameMap = newMap
		return
	}

	for _, mod := range m.Modules() {
		if mod == nil {
			continue
		}
		for _, obj := range mod.Objects() {
			if obj != nil && obj.OID() != nil {
				oidStr := obj.OID().String()
				if !strings.HasPrefix(oidStr, ".") {
					oidStr = "." + oidStr
				}
				name := obj.Name()
				if name != "" {
					newMap[oidStr] = name
				}
			}
		}
	}
	oidToNameMap = newMap
}

// ResolveOID returns the textual MIB object name for a given numeric OID string.
func ResolveOID(oid string) string {
	if oid == "" {
		return oid
	}

	cleanOID := oid
	if !strings.HasPrefix(cleanOID, ".") {
		cleanOID = "." + cleanOID
	}

	parts := strings.Split(cleanOID, ".")

	MibEngineMu.RLock()
	defer MibEngineMu.RUnlock()

	// Direct match
	if name, ok := oidToNameMap[cleanOID]; ok {
		return name
	}

	// Try matching base OID with suffix (e.g. ifIndex.1)
	for i := len(parts) - 1; i >= 2; i-- {
		baseOID := strings.Join(parts[:i], ".")
		suffix := strings.Join(parts[i:], ".")
		if name, ok := oidToNameMap[baseOID]; ok {
			return name + "." + suffix
		}
	}

	return oid
}
