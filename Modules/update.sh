#!/bin/bash

git fetch origin
git reset --hard origin/main

echo "Локальная ветка 'main' полностью синхронизирована с origin/main"