// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filelogreceiver

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type bodyMapConsumer struct {
	inner consumer.Logs
}

func newBodyMapConsumer(inner consumer.Logs) *bodyMapConsumer {
	return &bodyMapConsumer{inner: inner}
}

func (c *bodyMapConsumer) Capabilities() consumer.Capabilities {
	return c.inner.Capabilities()
}

func (c *bodyMapConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	applyBodyMapEncoding(ld)
	return c.inner.ConsumeLogs(ctx, ld)
}

func applyBodyMapEncoding(ld plog.Logs) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")
			for k := 0; k < sl.LogRecords().Len(); k++ {
				applyBodyMapRecord(sl.LogRecords().At(k))
			}
		}
	}
}

func applyBodyMapRecord(lr plog.LogRecord) {
	bm := pcommon.NewMap()

	bm.PutStr("@timestamp", lr.ObservedTimestamp().AsTime().UTC().Format(time.RFC3339Nano))

	switch lr.Body().Type() {
	case pcommon.ValueTypeStr:
		bm.PutStr("message", lr.Body().Str())
	case pcommon.ValueTypeEmpty:
		// no body
	default:
		lr.Body().CopyTo(bm.PutEmpty("message"))
	}

	lr.Attributes().Range(func(k string, v pcommon.Value) bool {
		switch k {
		case "log.file.name":
			// drop: filelog-only field with no filebeat equivalent
		case "log.file.record_offset":
			putNestedValue(bm, "log.offset", v)
		default:
			putNestedValue(bm, k, v)
		}
		return true
	})

	lr.Attributes().Clear()
	bm.CopyTo(lr.Body().SetEmptyMap())
}

// putNestedValue inserts v into dest using dottedKey as a nested path.
// "log.file.path" becomes dest["log"]["file"]["path"] = v.
func putNestedValue(dest pcommon.Map, dottedKey string, v pcommon.Value) {
	parent, rest, found := strings.Cut(dottedKey, ".")
	if !found {
		v.CopyTo(dest.PutEmpty(dottedKey))
		return
	}
	var child pcommon.Map
	if existing, ok := dest.Get(parent); ok && existing.Type() == pcommon.ValueTypeMap {
		child = existing.Map()
	} else {
		child = dest.PutEmpty(parent).SetEmptyMap()
	}
	putNestedValue(child, rest, v)
}
