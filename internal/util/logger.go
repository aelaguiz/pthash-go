package util

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Log logs a message if verbose is true.
func Log(verbose bool, format string, args ...any) {
	if verbose {
		log.Printf(format, args...)
	}
}

// ProgressLogger tracks and prints progress.
type ProgressLogger struct {
	totalEvents    uint64
	prefix         string
	suffix         string
	loggedEvents   uint64
	logStep        uint64
	nextEventToLog uint64
	enabled        bool
	startTime      time.Time
	lastUpdateTime time.Time
}

// NewProgressLogger creates a new progress logger.
func NewProgressLogger(totalEvents uint64, prefix, suffix string, enable bool) *ProgressLogger {
	pl := &ProgressLogger{
		totalEvents:  totalEvents,
		prefix:       prefix,
		suffix:       suffix,
		loggedEvents: 0,
		enabled:      enable,
		startTime:    time.Now(),
	}

	percFraction := uint64(20) // Default to 5% steps
	if totalEvents >= 100_000_000 {
		percFraction = 100 // 1% steps for large counts
	}
	pl.logStep = (totalEvents + percFraction - 1) / percFraction
	if pl.logStep == 0 {
		pl.logStep = 1
	}

	if enable {
		pl.nextEventToLog = pl.logStep
		pl.update(false) // Initial print
	} else {
		pl.nextEventToLog = ^uint64(0) // Effectively disable updates if !enable
	}
	return pl
}

// Log increments the counter and updates progress if the step is reached.
func (pl *ProgressLogger) Log() {
	if !pl.enabled {
		return
	}
	pl.loggedEvents++
	if pl.loggedEvents >= pl.nextEventToLog {
		pl.update(false)
		pl.nextEventToLog += pl.logStep
		// Ensure the last update on 100%
		if pl.nextEventToLog > pl.totalEvents {
			pl.nextEventToLog = pl.totalEvents
		}
	}
}

// Finalize prints the 100% progress update.
func (pl *ProgressLogger) Finalize() {
	if !pl.enabled {
		return
	}
	// Ensure loggedEvents matches totalEvents if finalizing early
	pl.loggedEvents = pl.totalEvents
	pl.update(true)
}

// update prints the progress status.
func (pl *ProgressLogger) update(final bool) {
	perc := uint64(0)
	if pl.totalEvents > 0 {
		perc = (100 * pl.loggedEvents) / pl.totalEvents
	}
	// Simple \r print, might not work well in all terminals/logs
	fmt.Printf("\r%s%d%%%s", pl.prefix, perc, pl.suffix)
	if final {
		elapsed := time.Since(pl.startTime)
		fmt.Printf(" (%.2fs) \n", elapsed.Seconds())
	} else {
		// Add throttling if updates are too frequent
		now := time.Now()
		if now.Sub(pl.lastUpdateTime) > 100*time.Millisecond { // Update max 10 times/sec
			fmt.Print(strings.Repeat(" ", 10)) // Add padding to clear previous line remnants
			fmt.Printf("\r%s%d%%%s", pl.prefix, perc, pl.suffix)
			pl.lastUpdateTime = now
		}
	}
}
