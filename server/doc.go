// Package server assembles one game-server process from typed configuration,
// generated data bindings, and independently selectable services.
//
// Server owns shared infrastructure and shutdown order. Project code remains
// responsible for parsing YAML, reading deployment identity, registering
// business handlers, and exposing narrow facades such as db.Save.
package server
