#!/bin/bash

# Remember to build your handler executable for Linux!
GOOS=linux GOARCH=amd64 go build -o main
zip main.zip main