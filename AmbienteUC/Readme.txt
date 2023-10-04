Acesso ao sistema linux

user: usermr
password: m2preduce

Configurar o ambiente MapReduce

executar os seguintes scripts pela seguinte ordem.
no fim da execuçao de cada script deve-se fechar a sessão ssh

00-a-java-install.sh
00-b-ant-install.sh
00-c-maven-install.sh
00-d-ssh-env.sh

installHadoop.sh

11-hadoop-InitUser.sh usermr

Os seguintes scripts podem ser todos executados na mesma sessão.

sudo chown -R usermr:hadoop examples/
sudo chmod -R o-w examples/

sudo chown usermr:hadoop /home/usermr

sempre que o contentor docker tiver de ser iniciado é necessário executar o seguinte script:

07-hadoopPseudoDistributed-Start.sh