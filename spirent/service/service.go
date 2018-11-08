package service

import (
	"github.com/SpirentOrion/luddite"
	"os"
)

type Config struct {
	Service luddite.ServiceConfig
}

type SpirentService struct {
	Service luddite.Service
	Started bool
}

var (
	service *SpirentService
)

func Service() *SpirentService {
	if service != nil {
		return service
	}
	var err error
	cfg := &Config{
		Service: luddite.ServiceConfig{
			Addr: ":9193",
			Version: struct {
				Min int
				Max int
			}{
				Min: 1,
				Max: 1,
			},
		},
	}
	cfgFile := os.Getenv("TELEGRAF_SPIRENT_CONF")
	if len(cfgFile) > 0 {
		if err := luddite.ReadConfig(cfgFile, &cfg); err != nil {
			panic(err)
		}
	}
	s, err := luddite.NewService(&cfg.Service)
	if err != nil {
		panic(err)
	}
	service = &SpirentService{
		Service: s,
	}
	return service
}

func (s *SpirentService) Start() {
	if !s.Started {
		go func() {
			s.Service.Logger().Info("Starting to listen on " + s.Service.Config().Addr)
			if err := s.Service.Run(); err != nil {
				s.Service.Logger().Error(err.Error())
			}
		}()
		s.Started = true
	}
}
