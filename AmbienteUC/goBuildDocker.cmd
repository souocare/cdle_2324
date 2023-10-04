@echo off
echo off

echo Removing old data...
docker builder prune --force

echo Creating CDLE image for Winter Semester 2023/2024...

cd Docker
	docker build -t cdle.ubuntu.2023.2024 .
cd ..

echo Creating container...
docker compose -f docker-compose-23-24.yml -p cdle-23-24 up -d

pause
