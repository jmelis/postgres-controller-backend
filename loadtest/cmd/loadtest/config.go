package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level load test specification.
type Config struct {
	Metadata           MetadataConfig `yaml:"metadata"`
	RDS                RDSConfig      `yaml:"rds"`
	Cluster            ClusterConfig  `yaml:"cluster"`
	Seed               SeedConfig     `yaml:"seed"`
	Phases             PhasesConfig   `yaml:"phases"`
	Verifier           VerifierConfig `yaml:"verifier"`
	CheckpointInterval time.Duration  `yaml:"checkpoint_interval"`
}

type MetadataConfig struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type RDSConfig struct {
	InstanceClass string `yaml:"instance_class"`
	EngineVersion string `yaml:"engine_version"`
}

type ClusterConfig struct {
	BaselinePollInterval time.Duration `yaml:"baseline_poll_interval"`
	DebounceFloor        time.Duration `yaml:"debounce_floor"`
}

type SeedConfig struct {
	GVKs []SeedGVK `yaml:"gvks"`
}

type SeedGVK struct {
	GVK              string `yaml:"gvk"`
	SpecSizeBytes    int    `yaml:"spec_size_bytes"`
	StatusSizeBytes  int    `yaml:"status_size_bytes"`
	MetadataSizeBytes int   `yaml:"metadata_size_bytes"`
	Objects          int    `yaml:"objects"`
}

type PhasesConfig struct {
	Phase0Baseline  Phase0Config   `yaml:"phase0_baseline"`
	Phase1Ceiling   Phase1Config   `yaml:"phase1_ceiling"`
	Phase2Steady    Phase2Config   `yaml:"phase2_steady"`
	Phase2bSkew     Phase2bConfig  `yaml:"phase2b_skew"`
	Phase3Avalanche Phase3Config   `yaml:"phase3_avalanche"`
	Phase5Poll      Phase5Config   `yaml:"phase5_poll"`
}

type Phase0Config struct {
	Enabled           bool `yaml:"enabled"`
	PingIterations    int  `yaml:"ping_iterations"`
	WriteIterations   int  `yaml:"write_iterations"`
	StoredProcWrites  int  `yaml:"stored_proc_writes"`
	SyncCommitCompare int  `yaml:"sync_commit_compare"`
}

type Phase1Config struct {
	Enabled          bool          `yaml:"enabled"`
	Workers          int           `yaml:"workers"`
	Duration         time.Duration `yaml:"duration"`
	WarmUp           time.Duration `yaml:"warm_up"`
	Runs             int           `yaml:"runs"`
	TargetRPS        float64       `yaml:"target_rps"`
	TargetP99Ms      int           `yaml:"target_p99_ms"`
	WorkerSweep      []int         `yaml:"worker_sweep"`
}

type Phase2Config struct {
	Enabled      bool          `yaml:"enabled"`
	Duration     time.Duration `yaml:"duration"`
	TargetRPS    float64       `yaml:"target_rps"`
	BurstRPS     float64       `yaml:"burst_rps"`
	TargetCPUPct int           `yaml:"target_cpu_pct"`
	TargetP50Ms  int           `yaml:"target_p50_ms"`
}

type Phase2bConfig struct {
	Enabled bool `yaml:"enabled"`
}

type Phase3Config struct {
	Enabled      bool    `yaml:"enabled"`
	KillFraction float64 `yaml:"kill_fraction"`
}

type Phase5Config struct {
	Enabled                bool          `yaml:"enabled"`
	NumWatchers            int           `yaml:"num_watchers"`
	WriteRate              int           `yaml:"write_rate"`
	WriteCount             int           `yaml:"write_count"`
	DoorbellDeliveryP99Ms  int           `yaml:"doorbell_delivery_p99_ms"`
	NotifyLossDrill        bool          `yaml:"notify_loss_drill"`
}

type VerifierConfig struct {
	Enabled        bool          `yaml:"enabled"`
	PollInterval   time.Duration `yaml:"poll_interval"`
	CanaryInterval time.Duration `yaml:"canary_interval"`
}

// LoadConfig reads and parses a YAML spec file into a Config.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// ComputeTotalObjects returns the total number of objects that will be seeded
// across all GVKs.
func (c *Config) ComputeTotalObjects() int {
	total := 0
	for _, g := range c.Seed.GVKs {
		total += g.Objects
	}
	return total
}

func (c *Config) validate() error {
	if c.Cluster.BaselinePollInterval <= 0 {
		c.Cluster.BaselinePollInterval = 5 * time.Second
	}
	if c.Cluster.DebounceFloor <= 0 {
		c.Cluster.DebounceFloor = 100 * time.Millisecond
	}
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 5 * time.Minute
	}
	p0 := &c.Phases.Phase0Baseline
	if p0.PingIterations <= 0 {
		p0.PingIterations = 1000
	}
	if p0.WriteIterations <= 0 {
		p0.WriteIterations = 500
	}
	if p0.StoredProcWrites <= 0 {
		p0.StoredProcWrites = 500
	}
	if p0.SyncCommitCompare <= 0 {
		p0.SyncCommitCompare = 200
	}
	p1 := &c.Phases.Phase1Ceiling
	if p1.WarmUp < 0 {
		p1.WarmUp = 0
	}
	if p1.Runs <= 0 {
		p1.Runs = 1
	}
	return nil
}
