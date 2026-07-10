// Command subscriber is the gRPC reference-data service. In Faz 2 it serves
// cell -> geo lookups from a static seed catalog; the fraud service calls it to
// evaluate the impossible-travel rule. (Subscriber profile / tariff lookups are
// added in a later phase.)
package main

import (
	"context"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdrv1 "github.com/ahaygun/go-cdr-fraud-detector/gen/cdr/v1"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/geo"
	"github.com/ahaygun/go-cdr-fraud-detector/internal/platform"
)

type server struct {
	cdrv1.UnimplementedReferenceServer
	log *slog.Logger
}

func (s *server) GetCell(_ context.Context, req *cdrv1.GetCellRequest) (*cdrv1.Cell, error) {
	c, ok := geo.Lookup(req.GetCellId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "cell %q not found", req.GetCellId())
	}
	return &cdrv1.Cell{CellId: c.ID, Lat: c.Lat, Lon: c.Lon, Name: c.Name}, nil
}

func main() {
	log := platform.NewLogger("subscriber")
	ctx, stop := platform.SignalContext(context.Background())
	defer stop()

	addr := platform.Getenv("GRPC_ADDR", ":50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("listen failed", "err", err)
		return
	}

	s := grpc.NewServer()
	cdrv1.RegisterReferenceServer(s, &server{log: log})

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		s.GracefulStop()
	}()

	log.Info("gRPC listening", "addr", addr, "cells", len(geo.Catalog))
	if err := s.Serve(lis); err != nil {
		log.Error("serve failed", "err", err)
	}
}
