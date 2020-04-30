package utils

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type KostyaConfig struct {
	KostyaColumnCounter int64 `mapstructure:"KostyaColumnCounter"`
}

var config KostyaConfig

func init() {
	config.AddToFlagSet(pflag.CommandLine)
	pflag.Parse()
}

// AddToFlagSet adds command line flags needed by the BenchmarkRunnerConfig to the flag set.
func (c KostyaConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.Int64("KostyaColumnCounter", 5000, "[Kostya-Author] size ('width') of table 'CPU' ")
}

func KostyaColumnCounter() int64 {
	return config.KostyaColumnCounter
}

// SetupConfigFile defines the settings for the configuration file support.
func SetupConfigFile() error {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	viper.BindPFlags(pflag.CommandLine)

	if err := viper.ReadInConfig(); err != nil {
		// Ignore error if config file not found.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	return nil
}
