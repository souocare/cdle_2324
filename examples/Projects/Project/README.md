Para facilitar também a gestão, assume-se que as pastas incluidas no zip estão dentro da pasta "/home/usermr/examples/Projects/Project", com todas as subpastas e ficheiros main.py e reviews.json nessa mesma pasta.

Para correr o main, é correr o comando:

python3 main.py local reviews.json "/home/usermr/examples/output/textanalysis/" 2 "GzipCodec"

ou

python3 main.py hadoop reviews.json "/home/usermr/examples/output/textanalysis/" 2 "GzipCodec"


Depois, basta seguir os passos.

----------------------------------------------------------------


Caso queira testar as tarefas de video e sentimento_dict, poderá abrir a respetiva pasta e correr o seguinte comando (testando a tarefa de video por exemplo):
python3 mapreduce.py file:///home/usermr/examples/Projects/Project/10_video/faces.mp4 -o file:///home/usermr/examples/output/textanalysis/