package main

import (
	"centauri/internal/app"
	"log"
)

func main() {
	log.Println("Starting application...")

	application := app.New()

	if err := application.Run(); err != nil {
		log.Fatalf("Application error: %v", err)
	}
}
