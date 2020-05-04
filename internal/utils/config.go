package utils

import (
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
	"os"
)

var kostyaColumnCounter int64

func PreConstructor() {
	log.Printf("[Config:Common] os.Args = %v", os.Args)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.Int64Var(&kostyaColumnCounter, "KostyaСountOfColumns", 5000, "[Kostya-Author] Counter of table columns for 'CPU' (default 5000)")
	flag.Parse()
	log.Printf("[Config:PreConstructor] kostyaColumnCounter = %d", kostyaColumnCounter)
}

func KostyaColumnCounter() int64 {
	return kostyaColumnCounter
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
