import cv2

video_path = "/home/usermr/examples/input/facedetect/faces.mp4"  # Replace with your actual video path
cap = cv2.VideoCapture(video_path)

frame_number = 0
while True:
    ret, frame = cap.read()
    if not ret:
        break

    frame_number += 1
    cv2.imwrite(f"frame_{frame_number}.png", frame)

cap.release()
