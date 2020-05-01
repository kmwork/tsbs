package utils

import (
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
)

var kostyaColumnCounter = flag.Int64("KostyaColumnCounter", 5000, "[Kostya-Author] Counter of table columns for 'CPU' (default 5000)")

func init() {
	log.Printf("[Config:Common] kostyaColumnCounter = %d", KostyaColumnCounter())
}

func KostyaColumnCounter() int64 {
	return *kostyaColumnCounter
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
