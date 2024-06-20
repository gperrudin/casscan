package casscanner

type Config struct {
	AutoSaveInterval int64
}

type Option func(*Config)

func WithAutoSaveInterval(interval int64) Option {
	return func(c *Config) {
		c.AutoSaveInterval = interval
	}
}
