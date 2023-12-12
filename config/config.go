package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

var Cfg Config

type Config struct {
	DB string `toml:"db"`
}

func Parse(p string) (err error) {
	_, err = toml.DecodeFile(p, &Cfg)
	if err != nil {
		err = fmt.Errorf("parse config error: %w", err)
		return
	}
	return
}
