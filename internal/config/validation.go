package config

import (
	"fmt"
	"strings"
)

// ValidationError represents a configuration validation error.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	var msgs []string
	for _, err := range e {
		msgs = append(msgs, err.Error())
	}
	return fmt.Sprintf("configuration validation failed: %s", strings.Join(msgs, "; "))
}

// Validate validates the configuration and returns any errors found.
func (c *Config) Validate() error {
	var errors ValidationErrors

	// Validate global config
	errors = append(errors, validateGlobal(&c.Global)...)

	// Validate queue config
	errors = append(errors, validateQueue(&c.Queue)...)

	// Validate eventsenders
	errors = append(errors, validateEventSenders(c.EventSenders)...)

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func validateGlobal(g *GlobalConfig) ValidationErrors {
	var errors ValidationErrors

	if g.DataDir == "" {
		errors = append(errors, ValidationError{
			Field:   "global.dataDir",
			Message: "data directory is required",
		})
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if g.LogLevel != "" && !validLogLevels[g.LogLevel] {
		errors = append(errors, ValidationError{
			Field:   "global.logLevel",
			Message: fmt.Sprintf("invalid log level %q, must be one of: debug, info, warn, error", g.LogLevel),
		})
	}

	validLogFormats := map[string]bool{"json": true, "text": true}
	if g.LogFormat != "" && !validLogFormats[g.LogFormat] {
		errors = append(errors, ValidationError{
			Field:   "global.logFormat",
			Message: fmt.Sprintf("invalid log format %q, must be one of: json, text", g.LogFormat),
		})
	}

	return errors
}

func validateQueue(q *QueueConfig) ValidationErrors {
	var errors ValidationErrors

	if q.EventsMaxAge.Duration < 0 {
		errors = append(errors, ValidationError{
			Field:   "queue.eventsMaxAge",
			Message: "events max age cannot be negative",
		})
	}

	if q.EventsMaxBytes.Sign() < 0 {
		errors = append(errors, ValidationError{
			Field:   "queue.eventsMaxBytes",
			Message: "events max bytes cannot be negative",
		})
	}

	return errors
}

func validateEventSenders(senders []EventSenderConfig) ValidationErrors {
	var errors ValidationErrors

	if len(senders) == 0 {
		errors = append(errors, ValidationError{
			Field:   "eventsenders",
			Message: "at least one event sender must be configured",
		})
		return errors
	}

	names := make(map[string]bool)
	for i, sender := range senders {
		prefix := fmt.Sprintf("eventsenders[%d]", i)

		if sender.Name == "" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".name",
				Message: "sender name is required",
			})
		} else if names[sender.Name] {
			errors = append(errors, ValidationError{
				Field:   prefix + ".name",
				Message: fmt.Sprintf("duplicate sender name %q", sender.Name),
			})
		} else {
			names[sender.Name] = true
		}

		if sender.Type == "" {
			errors = append(errors, ValidationError{
				Field:   prefix + ".type",
				Message: "sender type is required",
			})
		}

		if sender.Filter != nil {
			errors = append(errors, validateFilter(sender.Filter, prefix+".filter")...)
		}
	}

	return errors
}

func validateFilter(f *FilterConfig, prefix string) ValidationErrors {
	var errors ValidationErrors

	if f.HardRefreshInterval.Duration < 0 {
		errors = append(errors, ValidationError{
			Field:   prefix + ".hardRefreshInterval",
			Message: "hard refresh interval cannot be negative",
		})
	}

	return errors
}
