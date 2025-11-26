import sqlite3
import io
import hashlib
import socket
from datetime import date, datetime
import numpy as np
import pandas as pd
import streamlit as st
import json
from pathlib import Path

# ------------------------
# CONFIGURAÇÕES INICIAIS
# ------------------------

st.set_page_config(
    page_title="Controle de Propostas",
    page_icon="logo_evolve.png",
    layout="wide"
)

def get_user_ip():
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        return ip
    except:
        return "IP desconocido"
    
def get_version_info():
    file_path = Path(__file__).parent / "version.json"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "version": "0.0.0",
            "build": 0,
            "generatedAt": "",
            "description": ""
        }

version_info = get_version_info()

# ------------------------
# Funções de banco de dados
# ------------------------
def get_connection():
    return sqlite3.connect("propostas.db", check_same_thread=False)


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    # Tabela de propostas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS propostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            digitador TEXT NOT NULL,
            ade TEXT NOT NULL,
            cpf TEXT NOT NULL,
            data TEXT NOT NULL,
            parceiro TEXT NOT NULL,
            tipo_produto TEXT,
            valor REAL,
            banco TEXT NOT NULL
        );
        """
    )

    # Garante coluna tipo_produto mesmo se a tabela já existir de antes
    cur.execute("PRAGMA table_info(propostas);")
    cols = [r[1] for r in cur.fetchall()]
    if "tipo_produto" not in cols:
        cur.execute("ALTER TABLE propostas ADD COLUMN tipo_produto TEXT;")

    # Tabela de usuários (login)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE NOT NULL,
            nome_exibicao TEXT NOT NULL,
            senha_hash TEXT NOT NULL,
            perfil TEXT NOT NULL  -- 'admin' ou 'digitador'
        );
        """
    )

    # Tabela de logs de auditoria das propostas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS log_propostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposta_id INTEGER,
            acao TEXT NOT NULL,          -- INSERT, UPDATE, DELETE
            usuario TEXT NOT NULL,       -- login de quem fez
            timestamp TEXT NOT NULL,     -- data/hora da ação
            detalhes TEXT,               -- texto livre descrevendo a mudança
            FOREIGN KEY (proposta_id) REFERENCES propostas(id)
        );
        """
    )

    # Tabela de parceiros
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parceiros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            descricao TEXT UNIQUE NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1
        );
        """
    )

    # Tabela de bancos
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bancos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            descricao TEXT UNIQUE NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1
        );
        """
    )

    # Popula parceiros iniciais se estiver vazio
    cur.execute("SELECT COUNT(*) FROM parceiros;")
    if cur.fetchone()[0] == 0:
        parceiros_iniciais = [
            "Selecione o parceiro",
            "1@EVOLVE SOLUÇÕES LTDA",
            "2@JOSE WALTER PEREIRA BATISTA",
            "3@MARIA IEDA SAMICO CAVALCANTI NETA",
            "1001@FLAVIA CHRISTIANE GOMES DE SIQUEIRA",
            "1002@PAULO RAMON GOMES DA SILVA",
            "1003@MICHELI ROSE DOS SANTOS SILVA",
            "1004@JOSE ERIK DOS SANTOS SILVA",
            "1005@CELIA EMPRESTIMOS LTDA",
            "1006@ACTOS PROMOTORA LTDA",
            "1007@ROZILVA VIEIRA DOS SANTOS",
            "1010@GEOVANIA SANTOS BARBOSA DE MOURA",
            "1014@MEGA CRED SERVICOS FINANCEIROS LTDA",
            "1017@RENE R. DA SILVA SANTOS",
            "1023@MARLLEN KELLY FERREIRA DA SILVA",
            "1024@MANACES FRANCA DO NASCIMENTO JUNIOR",
            "1026@J J SOARES COSTA LTDA",
            "1027@EMILLY BREENDA DE FIGUEIREDO DA SILVA",
            "1030@EVOLVE PROMOTORA LTDA",
            "1031@ERIKA PATRICIA BEZERRA BRASIL",
            "1032@JANAINA RENATA DA SILVA",
            "1033@S C DA C FIGUEREDO NA CONTACRED",
            "1034@ISACREDITO CADASTRO E INTERMEDIACOES FINANCEIRAS LTDA",
            "1036@AD SANTANA RAMOS",
            "1038@R P DA SILVA PROMOTORA DE NEGOCIOS",
            "1039@F DOS SANTOS BARROS LTDA",
            "1040@JAQUELINE PEREIRA DA SILVA",
            "1041@ALAN FERREIRA CHICUTA",
            "1043@MERCIA LOPES DE GOES",
            "1044@EVERTON FELIX TELES",
            "1045@RAYSSA KARLA SILVA DOS SANTOS",
            "1046@MORAES CONSIGNADOS LTDA",
            "1047@VIVIANE RIBEIRO SENA DE MELO SILVA",
            "1048@Q L A CHUCUTA PROMOTORA DE VENDAS E SERVICOS",
            "1049@CLAUDETE OLIVEIRA BORGES DA SILVA",
            "1050@KEILA COSTA BARROS",
            "1052@ISRAEL BISPO DA SILVA",
            "1053@RAFAEL VICENTE DA SILVA SANTOS",
            "1054@SILVANIA ALMEIDA DA SILVA QUIRINO",
            "1055@CARLOS CESAR FOLHA DE SANTANA",
            "1056@FERNANDO SAMPAIO BARROS",
            "1057@KAUAI AMORIM GOUVEIA",
            "1059@FERNANDA EMANUELLE DOS SANTOS FERREIRA",
            "1060@VANIA LUCIA SILVESTRE DE ANDRADE CASTRO SOTERO",
            "1061@TANIA MARIA SANTOS FERREIRA",
            "1062@CINTHIA ALVES CADETE",
            "1063@VALERIA FERREIRA SILVA",
            "1064@KARLA JULIANA SOARES MACIEL",
            "1065@VALQUIRIA SANTOS",
            "1066@WARLLEN VINICIUS GAMA DE LIMA",
            "1067@CARLA NAIARA DOS SANTOS LIMA",
            "1068@GILSON NASCIMENTO DO CARMO",
            "1070@ADIVALDIR DOS SANTOS SIQUEIRA JUNIOR",
            "1072@ANDREIA DOS SANTOS SILVA",
            "1073@GUSTAVO ARGEMIRO DA SILVA PARREIRA",
            "1074@JOAO LINO RAMOS NETO",
            "1075@PAULA JULIANA CORDEIRO SANTOS DA SILVA",
            "1076@IZABELLA ALVES DE MENEZES MAIA",
            "1078@SELMA ALVES BULHOES DE MACEDO",
            "1079@SHEILA GUILHERME DA SILVA",
            "1080@SALETE MENDONCA PEREIRA",
            "1081@FERNANDA PESSOA DE OLIVEIRA",
            "1082@MARIA DE LOURDES GOMES LIMA",
            "1083@RAFAELLA DE ARAUJO CANDIDO DA SILVA",
            "1084@JOSE VITOR OLIVEIRA SILVA",
            "1086@ERICK GUSTAVO ALVES DE SOUZA",
            "1087@CLARISSA BARACHO PEREIRA",
            "1088@MARCOS PAULO TEIXEIRA DA SILVA",
            "1089@THIAGO WINICIUS NOGUEIRA DA SILVA",
            "1085@MONICA TALITA DA SILVA AMARO",
            "1091@RENATO DE SANTANA ALVES",
            "1092@AMANDA GALVAO DE SOUZA",
            "1093@NILTON FABRICIO SANTANA RAMOS",
            "1094@LUCIANA GOMES DA SILVA CRUZ",
            "1095@ALBERTO TEIXEIRA DE SOUZA FILHO",
            "1096@KAMILLA CAMPOS MALTA",
            "1097@FABIOLA VANIA PIMENTEL BRANDAO FREIRE",
            "1098@JANE GONCALVES DE MACEDO",
            "1099@ROBSON BERNARDO OLIVEIRA",
            "1100@HELLISVAN CLEMENTE DA SILVA",
            "1077@JACQUELINE SANTOS VICENTE DA SILVA",
            "1102@JOSYVANIA LINS SANTOS",
            "1103@INGRID MAYARA MOREIRA DE OLIVEIRA MENDONCA CANDEA",
            "1104@ANA LUCIA PEREIRA DA SILVA",
            "1105@ODJANE SILVA DOS SANTOS",
            "1106@LARISSA KALLYNE DOS SANTOS ALVES PEIXOTO",
            "1107@MICHAEL MORAES DE BARROS",
            "1108@MAICOLN SILVA SANTOS",
            "1109@JOYCE MAYARA BARBOSA DA SILVA",
            "1110@MARCEL DE MORAIS TENORIO",
            "1111@MICHELLE MONTELARES DE OLIVEIRA E SILVA",
            "1112@ERICK DAMIAO DOS SANTOS SILVA",
            "1113@ALINE DE MENDONCA ARAUJO",
            "1114@LEANDRO DE OLIVEIRA CAVALCANTE",
            "1115@JHOANNA LOWHAYNY DA SILVA SANTOS",
            "1116@MARIA DA PENHA VIEIRA DE FARIAS",
            "1117@QUEZIA ELOY TENORIO CADENGUE",
            "1118@JEAN CARLOS SILVA SANTANA",
            "1071@CICERO ALDO DOS SANTOS DA COSTA",
            "1120@JALDEMO OLIVEIRA PAZ",
            "1121@SAQUE CRED",
            "1122@INES MARINHO PEIXOTO",
            "1123@ALESSANDRO TEIXEIRA DA SILVA",
            "1124@LUCAS LINDO DE SOUZA",
            "1125@NELSON R DA SILVA FILHO",
            "1126@GUSTAVO DO AMARAL LIMA",
            "1127@JENESSON PEREIRA DA SILVA",
            "1128@DIVA HERMELINDA SANTOS DO NASCIMENTO",
            "1129@WANESSA ROBERTA VILA NOVA DA SILVA CALAZANS",
            "1130@MAIARA SANTOS DE MEDEIROS",
            "1131@JEFERSON WILLIAM COSTA VIEIRA",
            "1132@LAYS FERNANDA ROCHA DA SILVA",
            "1133@DMAIS PROMOTORA LTDA",
            "1134@DANIEL DANTAS NETO",
            "1069@PATRICIA DA COSTA SANTOS",
            "1136@ROSILEIDE GOMES DOS SANTOS",
            "1137@ALEXSANDRA LINDYSANE VANDERLEI SANTOS GUIMARAES",
            "1138@MARCEL DE MORAIS TENORIO JUNIOR",
            "1139@54.266.455 BRENA GABRIELE CHASTINET NASCIMENTO",
            "1140@P P DA S SANTOS TOPCRED SOLUCOES FINANCEIRAS",
            "1141@JOSE DOUGLAS CAVALCANTE DA ROCHA",
            "1058@CAMILA PEREIRA DA SILVA",
            "1143@S. PAULA GUIMARAES AMARAL",
            "1144@DEBORAH BEATRIZ SILVA SANTOS",
            "1145@ROSMYLE MONTEIRO DOS SANTOS",
            "1146@CICERO RAFAEL DOS SANTOS SILVA",
            "1147@FABIO TORRES MEDEIROS REGO",
            "1148@CLAUDIANE ARAUJO DA SILVA",
            "1149@ANDREIA DA SILVA SANTOS",
            "1150@LEONE PEREIRA GOMES",
            "1051@ANE ISABELY AZEVEDO DE SOUZA",
            "1042@SIVALDO CALIXTO DA ROCHA",
            "1154@GUILHERME TEIXEIRA TAVARES",
            "1155@RENATA DE FATIMA ANDRADE MASTRANGELI",
            "1156@HERICK DE MENDONCA ANSELMO",
            "1157@TAMMIRIS EMANUELA DOMINGOS DE MELO",
            "1158@JOSINEIDE MARIA DA SILVA SANTOS",
            "1159@MICHELINE KATTY DE LIMA",
            "1037@LUCINEIDE MARIA BARBOSA",
            "1161@RENATA MARIA DOS SANTOS",
            "1035@MARCELO CORTEZ DE LUCENA 00307167569",
            "1163@ALLYSON GUILHERME FELIX DO NASCIMENTO",
            "1164@RENATA BARROS DE CASTRO",
            "1165@DANIELA KIVIA GOMES NICANDRO",
            "1166@FLAVIA CRISTINA ASSUNCAO DE MELO",
            "1029@ALBUQUERQUE E MORAES SERVICOS LTDA",
            "1168@MARIA HELANIA DA SILVA",
            "1169@KETHELY ALVES",
            "1170@EDUARDO GOMES DA CRUZ",
            "1171@JOSIVANIA DA SILVA LUZ",
            "1172@MARILENE RODRIGUES GOMES DEODATO",
            "1173@MARIA FERNANDA GOMES DE LIMA FERNANDES",
            "1174@SILVANIA SARAIVA DE FRANCA SILVA",
            "1175@MARIA CLAUDIA GREGO DE AGUIAR LIMA",
            "1176@SHALLAKO WANDYSON MOREIRA DO CARMO",
            "1177@JULIO JOSE CLIMACO DE MELO MENDONCA",
            "1178@VANESSA RIBEIRO DOMINGOS SANTOS",
            "1179@JEFFERSON CAVALCANTI LUCENA",
            "1182@SAVIO ALLAN CABRAL SANTANA DA SILVA",
            "1028@FABRICIA SILVA DE BRITO CAMPOS",
            "1184@CARLA CLAUDIA GUILHERME DA SILVA MARQUES",
            "1185@NEXT LEVEL PROMOTORA LTDA",
            "1186@NEXT LEVEL PROMOTORA LTDA",
            "1189@MARIA NEIDE CAMARA DE QUEIROZ",
            "1190@ELIEZER ALVES DE MEIRELES",
            "1191@JACIRA DOS SANTOS FILGUEIRA",
            "1025@F P DE OLIVEIRA FERNANDES",
            "1193@ANDREA CHRISTIANE DE MENEZES ANDRADE",
            "1194@MONICA GOUVEIA DA SILVA SANTOS",
            "1195@MARIA MADALENA DA SILVA",
            "1196@LUZIANA MARIA DE SOUZA DUTRA",
            "1197@KARINY KELLY DE MENEZES",
            "1022@REAL PROMOTORA DE VENDAS LTDA",
            "1199@ALEXANDRA ALVES DE CARVALHO",
            "1200@SEVERINO EDUARDO DA SILVA",
            "1201@ISABELE MARIA BARBOSA SANTOS",
            "1202@ISMAEL DE SOUZA GOMES",
            "1203@VERA LUCIA ANDRADE DONATO",
            "1205@JOSE CID HONORATO",
            "1206@ADEILDO JOSE PATRIOTA",
            "1207@CLAUDIO CEZAR COUTINHO DE MOURA",
            "1208@JANIEIDE DA SILVA GONCALVES",
            "1021@DAYANE B S DE OLIVEIRA",
            "1020@LARISSA DAIANNY DA SILVA AVELINO",
            "1211@CLAUDIA MARIA FERREIRA CAMILO",
            "1212@FABIANE BENICIO DE ARAUJO",
            "1019@RIVANIA ROMANA RODRIGUES DO NASCIMENTO LTDA",
            "1214@MARIA CRISTINA ALVES BARBOSA",
            "1215@SUELEN LIMA DA SILVA",
            "1216@MARCELO DELGADO ALVES",
            "1018@ROSIMARIA BARBOSA DE FIGUEIREDO",
            "1218@CRISTIANE IZIDORO DA SILVA",
            "1219@IARA CURVELO TENORIO",
            "1220@PEDRO ANTONIO DA SILVA",
            "1221@MARIA ZENIVANIA ALVES BARBOSA MOURA",
            "1222@ANDRESSA RAYANE DOS SANTOS",
            "1224@MACIELE NOGUEIRA DE LIMA",
            "1225@MARIA IEDA SAMICO CAVALCANTI NETA",
            "1226@ANDERSON FERREIRA DE SOUZA",
            "1227@JOAO CARLOS TAVARES DE LIMA",
            "1228@GABRIELA ALENCAR CAMPELO DE MELO",
            "1229@CAMILA IRIS DE FRANCA SILVA",
            "1231@DANIELE CIARA DA SILVA",
            "1232@THAINA SILVA FERREIRA",
            "1233@MARCYVANIA SANTOS DA SILVA",
            "1016@KATIA CRISTINA MORAIS DE OLIVEIRA KM",
            "1235@VILMAR DONDI",
            "1236@LUIZ RAPHAEL D EMERY DUARTE",
            "1237@ERIKA FERNANDA MARTINS DA SILVA",
            "1238@MARCELA DE SOUZA COSTA",
            "1239@DUCILENE NOGUEIRA DA SILVA",
            "1240@IVINA ELENITA RIBEIRO BEZERRA",
            "1015@E DOS SANTOS REPRESENTACAO",
            "1242@CYNTHIA SANTOS SILVA",
            "1243@JOSE CICERO SILVA NETO",
            "1244@TESTE",
            "2244@PRODUÇÃO INTERNA",
            "2250@TABELA ESPELHO",
            "2251@IULY GOMES DOS SANTOS",
            "2252@M FINANCEIRA LTDA",
            "2257@FLAVIA REJANE DOS SANTOS",
            "2258@TABELA ESPELHO ACTOS",
            "2260@JOCYANE LUCIA SANTOS AMORIM",
            "2261@TABELA ESPELHO AYRON",
            "2262@TABELA ESPELHO C6BANK",
            "2263@TABELA ESPELHO BONUS PAN",
            "2264@CELSO CORREIA DE LIMA",
            "2266@RICARDO LEONARDO FERREIRA DA SILVA",
            "2267@ANDRE DO ESPIRITO SANTO CRUZ FILHO",
            "2268@TABELA ESPELHO SAQUECRED",
            "2269@GLAUCIA ALVES SANTOS",
            "2270@JESSICA GOUVEIA DA SILVA",
            "2271@TABELA ESPELHO SAQUE CRED",
            "2272@TESTE 2",
            "1013@ROSALIA NUNES DA CRUZ",
            "1012@MARCIO ANDRE PAES BENJOINO FILHO",
            "1011@DENISE VANDERLEI LUCAS 80309933404",
            "1009@SILVANIA RAMOS DOS SANTOS LIMA",
            "1008@FRANCO CARNAUBA FERREIRA",
            "1090@VANIA LUCIA DE LIMA RODRIGUES",
            "1101@BEMSENIOR PROMOTORA DE CREDITO LTDA",
            "1119@LETICIA DAS NEVES DOS SANTOS",
            "1135@ROSILEIDE FERNANDES BARBOZA SILVA",
            "1142@MARCELO DOS SANTOS BEZERRA",
            "1151@MARIA ELIANE CRISTIANO LAURINDO",
            "1152@TILGATHPILNEZER FERNANDES LIMA NETO",
            "1160@VANDETE TAVARES DA SILVA PEREIRA",
            "1162@ICARO LEON UBIRATAN SILVA",
            "1167@MARISTELA RODRIGUES LEMOS",
            "1183@JOSE NAZARENO TELES",
            "1192@EDUARDO LOPES DO NASCIMENTO",
            "1198@JOSE ANDERSON DO NASCIMENTO",
            "1209@LUCIA DE FATIMA DA CONCEICAO",
            "1210@ANA MARIA ARAGAO",
            "1213@LUANA DA SILVA RAMOS",
            "1217@TANIA LUZIMAR DA SILVA PEREIRA",
            "1234@JUSCARA LIMA SOARES",
            "1241@NEIDJA ABREU DE QUEIROZ",
        ]
        for p in parceiros_iniciais:
            cur.execute(
                "INSERT OR IGNORE INTO parceiros (descricao, ativo) VALUES (?, 1);",
                (p,),
            )

    # Popula bancos iniciais se estiver vazio
    cur.execute("SELECT COUNT(*) FROM bancos;")
    if cur.fetchone()[0] == 0:
        bancos_iniciais = [
            "C6 - DG",
            "DIGIO - BEVI",
            "DIGIO - DG (209271)",
            "DIGIO - AD PROM.(100154)",
            "BMG - DG 53991",
            "BMG - BEVI",
            "FACTA FINANCEIRA - DG",
            "OLE(ANTIGO) - DG",
            "OLE(FVE) - DG",
            "SANTANDER - DG",
            "CREFISA - DG",
            "CREFISA BOLSA FAMILIA - DG",
            "CREFISA BOLSA FAMILIA - ALCIF",
            "CIASPREV - INOPERANTE",
            "PAULISTA - INOPERANTE",
            "VEMCARD - INOPERANTE",
            "HAPPY - DG",
            "AMIGOZ - DG",
            "ITAU - DG",
            "BRB - ESTEIRA DIGITAL",
            "BRB - EVOLVE",
            "BRB - DG",
            "CREFAZ - DG",
            "SABEMI - BEVI",
            "QUERO+ - DG",
            "QUERO+ (RL) - DG",
            "MASTER - DG",
            "INCONTA - PORT",
            "INBURSA - PORT",
            "DAYCOVAL - BEVI",
            "BANRISUL - BEVI",
            "BANRISUL - DG",
            "SAFRA - BEVI",
            "SAFRA - DIRETO",
            "MEU CASHCARD",
            "KARDBANK - GFT",
            "ICRED - BEVI",
            "MERCANTIL - DG",
            "FINANTO - DIRETO",
            "PRESENÇA BANK - DG",
            "FUTURO PREVIDENCIA - DIRETO",
            "AKI CAPITAL (ALCIF CONVENIOS) - ALCIF",
            "PAN - DG",
            "PICPAY - DG",
            "PARANA - DG",
            "NBC BANK - DG",
            "PRATA - DG",
        ]
        for b in bancos_iniciais:
            cur.execute(
                "INSERT OR IGNORE INTO bancos (descricao, ativo) VALUES (?, 1);",
                (b,),
            )

    conn.commit()
    conn.close()
    criar_usuario_inicial()


def hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()


def criar_usuario_inicial():
    """
    Cria um usuário admin padrão caso a tabela esteja vazia.
    usuario: admin / senha: admin
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM usuarios;")
    (qtd,) = cur.fetchone()
    if qtd == 0:
        cur.execute(
            """
            INSERT INTO usuarios (usuario, nome_exibicao, senha_hash, perfil)
            VALUES (?, ?, ?, ?);
            """,
            ("admin", "Administrador", hash_senha("admin"), "admin"),
        )
        conn.commit()
    conn.close()


def autenticar_usuario(usuario: str, senha: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, usuario, nome_exibicao, senha_hash, perfil FROM usuarios WHERE usuario = ?;",
        (usuario,),
    )
    row = cur.fetchone()
    conn.close()

    if row is None:
        return None

    user_id, usr, nome_exibicao, senha_hash_db, perfil = row
    if senha_hash_db == hash_senha(senha):
        return {
            "id": user_id,
            "usuario": usr,
            "nome_exibicao": nome_exibicao,
            "perfil": perfil,
        }
    return None


def listar_usuarios():
    """Retorna um DataFrame com os usuários (sem mostrar hash da senha)."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT id, usuario, nome_exibicao, perfil FROM usuarios ORDER BY id;",
        conn
    )
    conn.close()
    return df


def registrar_log_proposta(acao: str, usuario: str, proposta_id: int | None = None, detalhes: str | None = None):
    """
    Registra uma ação (INSERT, UPDATE, DELETE) relacionada a uma proposta.
    - acao: 'INSERT', 'UPDATE', 'DELETE'
    - usuario: login de quem fez a ação
    - proposta_id: id da proposta (pode ser None, mas é bom sempre mandar)
    - detalhes: texto livre descrevendo o que aconteceu
    """
    conn = get_connection()
    cur = conn.cursor()
    ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    cur.execute(
        """
        INSERT INTO log_propostas (proposta_id, acao, usuario, timestamp, detalhes)
        VALUES (?, ?, ?, ?, ?);
        """,
        (proposta_id, acao, usuario, ts, detalhes),
    )
    conn.commit()
    conn.close()


def criar_usuario(usuario: str, nome_exibicao: str, senha: str, perfil: str):
    """Cria um novo usuário com senha hasheada."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO usuarios (usuario, nome_exibicao, senha_hash, perfil)
        VALUES (?, ?, ?, ?);
        """,
        (usuario, nome_exibicao, hash_senha(senha), perfil),
    )
    conn.commit()
    conn.close()


def excluir_usuario(user_id: int):
    """Exclui usuário pelo ID (não permite apagar o admin padrão)."""
    conn = get_connection()
    cur = conn.cursor()
    # Garante que não vai apagar o usuario 'admin'
    cur.execute("SELECT usuario FROM usuarios WHERE id = ?;", (user_id,))
    row = cur.fetchone()
    if row is not None:
        if row[0] == "admin":
            conn.close()
            raise ValueError("Não é permitido excluir o usuário 'admin'.")
    cur.execute("DELETE FROM usuarios WHERE id = ?;", (user_id,))
    conn.commit()
    conn.close()


def inserir_proposta(digitador, ade, cpf, data_str, parceiro, tipo_produto, valor, banco):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO propostas (digitador, ade, cpf, data, parceiro, tipo_produto, valor, banco)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (digitador, ade, cpf, data_str, parceiro, tipo_produto, valor, banco),
    )
    nova_id = cur.lastrowid
    conn.commit()
    conn.close()
    return nova_id


def atualizar_proposta(id_proposta, digitador, ade, cpf, data_str, parceiro, tipo_produto, valor, banco):
    """Atualiza uma proposta existente pelo ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE propostas
        SET digitador = ?, ade = ?, cpf = ?, data = ?, parceiro = ?, tipo_produto = ?, valor = ?, banco = ?
        WHERE id = ?;
        """,
        (digitador, ade, cpf, data_str, parceiro, tipo_produto, valor, banco, id_proposta),
    )
    conn.commit()
    conn.close()


def excluir_proposta_bd(id_proposta):
    """Exclui uma proposta pelo ID."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM propostas WHERE id = ?;", (id_proposta,))
    conn.commit()
    conn.close()


def carregar_propostas():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM propostas ORDER BY id DESC;", conn)
    conn.close()
    return df


# --------------------------
# Parceiros / Bancos - Funções de apoio
# --------------------------
def listar_parceiros_bd():
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT id, descricao, ativo FROM parceiros ORDER BY descricao;",
        conn
    )
    conn.close()
    return df


def listar_bancos_bd():
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT id, descricao, ativo FROM bancos ORDER BY descricao;",
        conn
    )
    conn.close()
    return df


def inserir_parceiro(descricao: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO parceiros (descricao, ativo) VALUES (?, 1);",
        (descricao.strip(),),
    )
    conn.commit()
    conn.close()


def alterar_status_parceiro(parceiro_id: int, ativo: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE parceiros SET ativo = ? WHERE id = ?;",
        (ativo, parceiro_id),
    )
    conn.commit()
    conn.close()


def excluir_parceiro(parceiro_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM parceiros WHERE id = ?;", (parceiro_id,))
    conn.commit()
    conn.close()


def inserir_banco(descricao: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bancos (descricao, ativo) VALUES (?, 1);",
        (descricao.strip(),),
    )
    conn.commit()
    conn.close()


def alterar_status_banco(banco_id: int, ativo: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE bancos SET ativo = ? WHERE id = ?;",
        (ativo, banco_id),
    )
    conn.commit()
    conn.close()


def excluir_banco(banco_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM bancos WHERE id = ?;", (banco_id,))
    conn.commit()
    conn.close()


def get_parceiros_opcoes():
    """Retorna lista para selectbox, com 'Selecione o parceiro' na frente."""
    df = listar_parceiros_bd()
    ativos = df[df["ativo"] == 1]["descricao"].sort_values().tolist()
    return ["Selecione o parceiro"] + ativos


def get_bancos_opcoes():
    """Retorna lista para selectbox, com 'Selecione o banco' na frente."""
    df = listar_bancos_bd()
    ativos = df[df["ativo"] == 1]["descricao"].sort_values().tolist()
    return ["Selecione o banco"] + ativos


# Inicializa o banco / tabelas
init_db()

# ------------------------
# STATE DE LOGIN
# ------------------------
if "usuario" not in st.session_state:
    st.session_state["usuario"] = None

usuario_logado = st.session_state["usuario"]

def get_user_ip():
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        return ip
    except:
        return "IP desconhecido"

ip_usuario = get_user_ip()


# ------------------------
# SIDEBAR - LOGIN E MENU
# ------------------------
with st.sidebar:
    # cria 3 colunas e usa a do meio para centralizar
    col_logo1, col_logo2, col_logo3 = st.columns([1, 2, 1])

    with col_logo2:
        st.image("logo_evolvegrande.png", use_container_width=True)

    st.markdown("---")

    st.markdown("### 🔐 Login")

    if usuario_logado is None:
        usuario_input = st.text_input("Usuário")
        senha_input = st.text_input("Senha", type="password")

        if st.button("Entrar"):
            user = autenticar_usuario(usuario_input.strip(), senha_input.strip())
            if user:
                st.session_state["usuario"] = user
                st.success(f"Bem-vindo, {user['nome_exibicao']}!")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")
        st.stop()  # Não mostra o resto da app enquanto não logar

    else:
        st.markdown(
            f"**Logado como:** {usuario_logado['nome_exibicao']}  \n"
            f"Perfil: `{usuario_logado['perfil']}`"
        )
        if st.button("Sair"):
            st.session_state["usuario"] = None
            st.rerun()

        st.write("---")

        # opções de menu variam conforme o perfil
        if usuario_logado["perfil"] == "admin":
            opcoes_menu = [
                "📄 Lançamento de Propostas",
                "📊 Dashboard",
                "📁 Consultas / Relatórios",
                "👥 Usuários",
                "🕒 Logs de Auditoria",
                "📈 Performance por Digitador",
                "🤝 Cadastro de Parceiros",
                "🏦 Cadastro de Bancos",
            ]
        else:
            opcoes_menu = [
                "📄 Lançamento de Propostas",
                "📊 Dashboard",
                "📁 Consultas / Relatórios",
            ]

        menu = st.radio("Menu", opcoes_menu)

# Se não tiver logado, já terá dado st.stop() acima
usuario_logado = st.session_state["usuario"]
digitador_logado = usuario_logado["nome_exibicao"]  # será usado como Digitador

# ------------------------
# TELA 1 - LANÇAMENTO DE PROPOSTAS
# ------------------------
if menu == "📄 Lançamento de Propostas":
    st.subheader("📝 Lançamento de Propostas")
    parceiros_opcoes = get_parceiros_opcoes()
    bancos_opcoes = get_bancos_opcoes()

    tipos_produto_lista = ["Selecione um Produto", "NOVO INSS", "REFIN", "CARTÃO", "FGTS", "SAQUE COMPLEMENTAR", "NOVO - CONVENIO PUBLICO", "REFIN - CONVENIO PUBLICO", "NOVO - AUMENTO", "SEGURO DE VIDA", "CREDITO PESSOAL", "CLT"]

      # ---------------------------
    # LINHA 1: Digitador | Data | Valor
    # ---------------------------
    col1, col2, col3 = st.columns(3)

    with col1:
        st.text_input(
            "Digitador (do login)",
            value=digitador_logado,
            disabled=True,
        )

    with col2:
        data_proposta = st.date_input(
            "Data da proposta",
            value=date.today(),
        )

    with col3:
        valor_str = st.text_input(
            "Valor da proposta",
            placeholder="Ex: 1500,00 (pode usar vírgula ou ponto)",
        )
        valor = None
        if valor_str.strip() != "":
            try:
                valor = float(valor_str.replace(".", "").replace(",", "."))
            except ValueError:
                st.warning("⚠️ Valor inválido. Use apenas números, vírgula e ponto.")

    # ---------------------------
    # LINHA 2: ADE | Tipo de produto | Banco
    # ---------------------------
    col1, col2, col3 = st.columns(3)

    with col1:
        ade = st.text_input("ADE", placeholder="Número da ADE")

    with col2:
        tipo_produto = st.selectbox(
            "Tipo de produto",
            tipos_produto_lista,
            index=0,
            key="novo_tipo_produto",
        )

    with col3:
        banco = st.selectbox(
            "Banco",
            bancos_opcoes,
            index=0,
            key="novo_banco",
        )

    # ---------------------------
    # LINHA 3: CPF | Parceiro
    # ---------------------------
    col1, col2, _ = st.columns(3)

    with col1:
        cpf = st.text_input("CPF do cliente", placeholder="Somente números")

    with col2:
        parceiro = st.selectbox(
            "Parceiro",
            parceiros_opcoes,
            index=0,
            key="novo_parceiro",
        )
    if st.button("Salvar proposta"):
        erros = []

        if not ade.strip():
            erros.append("Preencher o campo ADE.")
        if not cpf.strip():
            erros.append("Preencher o campo CPF.")
        if parceiro == "Selecione o parceiro":
            erros.append("Selecionar um parceiro.")
        if banco == "Selecione o banco":
            erros.append("Selecionar um banco.")

        if erros:
            st.error("⚠️ Verifique os campos obrigatórios:\n- " + "\n- ".join(erros))
        else:
            try:
                # monta data válida
                data_str = data_proposta.strftime("%Y-%m-%d")
                valor_num = valor if valor is not None else None

                # insere proposta e CAPTURA O ID!!
                nova_id = inserir_proposta(
                    digitador=digitador_logado.strip(),
                    ade=ade.strip(),
                    cpf=cpf.strip(),
                    data_str=data_str,
                    parceiro=parceiro.strip(),
                    tipo_produto=tipo_produto,
                    valor=valor_num,
                    banco=banco.strip(),
                )

                # 🔥 Registro no LOG
                try:
                    registrar_log_proposta(
                        acao="INSERT",
                        usuario=usuario_logado["usuario"],
                        proposta_id=nova_id,
                        detalhes=(
                            f"digitador={digitador_logado.strip()}, "
                            f"ade={ade.strip()}, cpf={cpf.strip()}, data={data_str}, "
                            f"parceiro={parceiro.strip()}, tipo_produto={tipo_produto}, "
                            f"valor={valor_num}, banco={banco.strip()}"
                        ),
                    )
                except Exception as e_log:
                    st.warning(f"⚠️ Proposta salva, mas não foi possível registrar o log: {e_log}")

                st.success("✅ Proposta salva com sucesso!")
                st.info(
                    "Você pode lançar várias propostas com o mesmo CPF e ADEs diferentes.\n"
                    "No cálculo de valores, quando o CPF tiver mais de uma proposta, o sistema ignora o campo VALOR."
                )

            except Exception as e:
                st.error(f"❌ Erro ao salvar proposta: {e}")

# ------------------------
# TELA 2 - DASHBOARD (INDICADORES + GRÁFICOS)
# ------------------------
elif menu == "📊 Dashboard":
    st.subheader("📊 Dashboard")

    df_all = carregar_propostas()

    if df_all.empty:
        st.info("Ainda não há propostas cadastradas.")
    else:
        # Garante coluna de data como datetime
        df_all["data_dt"] = pd.to_datetime(df_all["data"], errors="coerce")
        data_min = df_all["data_dt"].min()
        data_max = df_all["data_dt"].max()

        if pd.isna(data_min) or pd.isna(data_max):
            data_min = data_max = date.today()

        st.markdown("### 🔎 Filtros (Dashboard)")

        # Linha 1: datas
        cold1, cold2, _, _ = st.columns(4)
        with cold1:
            data_inicial = st.date_input(
                "Data inicial",
                value=data_min.date(),
                key="dash_data_ini"
            )
        with cold2:
            data_final = st.date_input(
                "Data final",
                value=data_max.date(),
                key="dash_data_fim"
            )

        # Linha 2: digitador, parceiro, banco, tipo_produto
        colf1, colf2, colf3, colf4 = st.columns(4)

        with colf1:
            filtro_digitador = st.text_input(
                "Digitador (nome contém)",
                placeholder="Opcional",
                key="dash_digitador"
            )

        parceiros_opcoes = get_parceiros_opcoes()
        bancos_opcoes = get_bancos_opcoes()
        tipos_produto_lista = ["Todos", "NOVO INSS", "REFIN", "CARTÃO", "FGTS", "SAQUE COMPLEMENTAR", "NOVO - CONVENIO PUBLICO", "REFIN - CONVENIO PUBLICO", "NOVO - AUMENTO", "SEGURO DE VIDA", "CREDITO PESSOAL", "CLT"]

        with colf2:
            filtro_parceiro_sel = st.selectbox(
                "Parceiro",
                ["Todos"] + parceiros_opcoes[1:],
                key="dash_parceiro"
            )

        with colf3:
            filtro_banco_sel = st.selectbox(
                "Banco",
                ["Todos"] + bancos_opcoes[1:],
                key="dash_banco"
            )

        with colf4:
            filtro_tipo_produto = st.selectbox(
                "Tipo de produto",
                tipos_produto_lista,
                key="dash_tipo_produto"
            )

        # Linha 3: CPF
        colf5, _, _, _ = st.columns(4)
        with colf5:
            filtro_cpf = st.text_input(
                "CPF (contém)",
                placeholder="Opcional",
                key="dash_cpf"
            )

        # -------------------------
        # Aplica filtros
        # -------------------------
        df = df_all.copy()

        # filtro por data
        df = df[
            (df["data_dt"].dt.date >= data_inicial) &
            (df["data_dt"].dt.date <= data_final)
        ]

        if filtro_digitador.strip():
            df = df[
                df["digitador"].str.contains(filtro_digitador.strip(), case=False, na=False)
            ]

        if filtro_parceiro_sel != "Todos":
            df = df[df["parceiro"] == filtro_parceiro_sel]

        if filtro_banco_sel != "Todos":
            df = df[df["banco"] == filtro_banco_sel]

        if filtro_tipo_produto != "Todos":
            df = df[df["tipo_produto"] == filtro_tipo_produto]

        if filtro_cpf.strip():
            df = df[
                df["cpf"].astype(str).str.contains(filtro_cpf.strip(), na=False)
            ]

        # Regra de CPF com mais de uma proposta (base geral)
        cpf_counts = df_all["cpf"].value_counts()
        df["ignorar_valor"] = df["cpf"].map(lambda c: cpf_counts.get(c, 0) > 1)
        df["valor_considerado"] = np.where(
            df["ignorar_valor"],
            0,
            df["valor"].fillna(0)
        )

        if df.empty:
            st.warning("Nenhum dado para exibir no dashboard com os filtros selecionados.")
        else:
            total_valor_bruto = df["valor"].fillna(0).sum()
            total_valor_considerado = df["valor_considerado"].sum()

            colr1, colr2 = st.columns(2)
            with colr1:
                st.metric(
                    "Soma dos valores (bruto)",
                    f"R$ {total_valor_bruto:,.2f}"
                )
            with colr2:
                st.metric(
                    "Soma dos valores (considerando regra de CPF)",
                    f"R$ {total_valor_considerado:,.2f}"
                )

            # ============================
            # GRÁFICOS
            # ============================
            st.markdown("### 📊 Gráficos")

            df_graf = df.copy()
            df_graf["data_dt"] = pd.to_datetime(df_graf["data"], errors="coerce")

            colg1, colg2 = st.columns(2)

            # Produção por dia
            with colg1:
                prod_dia = (
                    df_graf.dropna(subset=["data_dt"])
                          .groupby(df_graf["data_dt"].dt.date)["valor_considerado"]
                          .sum()
                          .reset_index(name="Valor Considerado")
                )
                prod_dia = prod_dia.sort_values("data_dt")

                if not prod_dia.empty:
                    st.markdown("**Produção por dia (Valor Considerado)**")
                    st.line_chart(prod_dia.set_index("data_dt")["Valor Considerado"])
                else:
                    st.info("Sem dados suficientes para gráfico por dia.")

            # Produção por banco
            with colg2:
                prod_banco = (
                    df_graf.groupby("banco")["valor_considerado"]
                           .sum()
                           .sort_values(ascending=False)
                           .head(10)
                           .reset_index()
                )

                if not prod_banco.empty:
                    st.markdown("**Top 10 Bancos por Valor Considerado**")
                    st.bar_chart(prod_banco.set_index("banco")["valor_considerado"])
                else:
                    st.info("Sem dados suficientes para gráfico por banco.")

            colg3, colg4 = st.columns(2)

            # Produção por parceiro
            with colg3:
                prod_parceiro = (
                    df_graf.groupby("parceiro")["valor_considerado"]
                           .sum()
                           .sort_values(ascending=False)
                           .head(10)
                           .reset_index()
                )

                if not prod_parceiro.empty:
                    st.markdown("**Top 10 Parceiros por Valor Considerado**")
                    st.bar_chart(prod_parceiro.set_index("parceiro")["valor_considerado"])
                else:
                    st.info("Sem dados suficientes para gráfico por parceiro.")

            # Produção por digitador
            with colg4:
                prod_digitador = (
                    df_graf.groupby("digitador")["valor_considerado"]
                           .sum()
                           .sort_values(ascending=False)
                           .reset_index()
                )

                if not prod_digitador.empty:
                    st.markdown("**Produção por Digitador (Valor Considerado)**")
                    st.bar_chart(prod_digitador.set_index("digitador")["valor_considerado"])
                else:
                    st.info("Sem dados suficientes para gráfico por digitador.")

            # Nova linha de gráficos: produção por tipo de produto
            colg5, _ = st.columns(2)
            with colg5:
                prod_tipo = (
                    df_graf.groupby("tipo_produto")["valor_considerado"]
                           .sum()
                           .sort_values(ascending=False)
                           .reset_index()
                )
                if not prod_tipo.empty:
                    st.markdown("**Produção por Tipo de Produto (Valor Considerado)**")
                    st.bar_chart(prod_tipo.set_index("tipo_produto")["valor_considerado"])
                else:
                    st.info("Sem dados suficientes para gráfico por tipo de produto.")

            st.caption(
                "Regra aplicada no dashboard: quando um CPF tem **mais de uma proposta** na base, "
                "o sistema **ignora o campo VALOR** (coluna 'Valor Considerado' = 0) para esse cliente."
            )

# ------------------------
# TELA - PERFORMANCE POR DIGITADOR
# ------------------------
elif menu == "📈 Performance por Digitador":
    st.subheader("📈 Performance por Digitador")

    df_all = carregar_propostas()

    if df_all.empty:
        st.info("Ainda não há propostas cadastradas.")
    else:
        # Trata datas
        df_all["data_dt"] = pd.to_datetime(df_all["data"], errors="coerce")
        data_min = df_all["data_dt"].min()
        data_max = df_all["data_dt"].max()

        if pd.isna(data_min) or pd.isna(data_max):
            data_min = data_max = date.today()

        # Listas de apoio para filtros
        parceiros_opcoes = get_parceiros_opcoes()
        bancos_opcoes = get_bancos_opcoes()
        tipos_produto_opcoes = ["Todos", "NOVO INSS", "REFIN", "CARTÃO", "FGTS", "SAQUE COMPLEMENTAR", "NOVO - CONVENIO PUBLICO", "REFIN - CONVENIO PUBLICO", "NOVO - AUMENTO", "SEGURO DE VIDA", "CREDITO PESSOAL", "CLT"]

        st.markdown("### 🔎 Filtros de Performance")

        # Linha 1 – intervalo de datas
        colf1, colf2 = st.columns(2)
        with colf1:
            data_inicial = st.date_input(
                "Data inicial",
                value=data_min.date(),
                key="perf_data_ini",
            )
        with colf2:
            data_final = st.date_input(
                "Data final",
                value=data_max.date(),
                key="perf_data_fim",
            )

        # Linha 2 – Banco / Parceiro / Tipo de produto / CPF
        colf3, colf4, colf5, colf6 = st.columns(4)
        with colf3:
            filtro_banco = st.selectbox(
                "Banco",
                ["Todos"] + bancos_opcoes[1:],
                key="perf_banco",
            )
        with colf4:
            filtro_parceiro = st.selectbox(
                "Parceiro",
                ["Todos"] + parceiros_opcoes[1:],
                key="perf_parceiro",
            )
        with colf5:
            filtro_tipo_produto = st.selectbox(
                "Tipo de produto",
                tipos_produto_opcoes,
                key="perf_tipo_produto",
            )
        with colf6:
            filtro_cpf = st.text_input(
                "Filtrar por CPF (contém)",
                placeholder="Opcional",
                key="perf_cpf",
            )

        # Linha 3 – filtro textual de digitador
        colf7, _ = st.columns([2, 2])
        with colf7:
            filtro_digitador = st.text_input(
                "Filtrar digitador (nome contém)",
                placeholder="Opcional",
                key="perf_digitador",
            )

        # --------------------------
        # Aplica filtros na base
        # --------------------------
        df = df_all.copy()
        df["data_dt"] = pd.to_datetime(df["data"], errors="coerce")

        # Filtro por data
        df = df[
            (df["data_dt"].dt.date >= data_inicial) &
            (df["data_dt"].dt.date <= data_final)
        ]

        # Filtros opcionais
        if filtro_digitador.strip():
            df = df[df["digitador"].str.contains(filtro_digitador.strip(), case=False, na=False)]

        if filtro_parceiro != "Todos":
            df = df[df["parceiro"] == filtro_parceiro]

        if filtro_banco != "Todos":
            df = df[df["banco"] == filtro_banco]

        if filtro_tipo_produto != "Todos":
            df = df[df["tipo_produto"] == filtro_tipo_produto]

        if filtro_cpf.strip():
            df = df[df["cpf"].str.contains(filtro_cpf.strip(), case=False, na=False)]

        if df.empty:
            st.warning("Nenhum dado para exibir com os filtros selecionados.")
        else:
            # Regra de CPF com mais de uma proposta (na base filtrada)
            cpf_counts = df["cpf"].value_counts()
            df["ignorar_valor"] = df["cpf"].map(lambda c: cpf_counts.get(c, 0) > 1)
            df["valor_considerado"] = np.where(
                df["ignorar_valor"],
                0,
                df["valor"].fillna(0),
            )

            # --------------------------
            # Agrupamento por digitador
            # --------------------------
            df_perf = (
                df.groupby("digitador")
                .agg(
                    qtd_propostas=("id", "count"),
                    qtd_clientes=("cpf", lambda x: x.nunique()),
                    valor_bruto=("valor", lambda x: x.fillna(0).sum()),
                    valor_considerado=("valor_considerado", "sum"),
                )
                .reset_index()
            )

            # Evita divisão por zero
            df_perf["ticket_medio"] = np.where(
                df_perf["qtd_propostas"] > 0,
                df_perf["valor_considerado"] / df_perf["qtd_propostas"],
                0,
            )

            # Ordena por valor considerado (do maior para o menor)
            df_perf = df_perf.sort_values("valor_considerado", ascending=False)

            st.markdown("### 📋 Tabela de performance por digitador")
            st.dataframe(
                df_perf.style.format(
                    {
                        "valor_bruto": "R$ {:,.2f}",
                        "valor_considerado": "R$ {:,.2f}",
                        "ticket_medio": "R$ {:,.2f}",
                    }
                ),
                use_container_width=True,
            )

            # --------------------------
            # Indicadores gerais
            # --------------------------
            total_valor_considerado = df_perf["valor_considerado"].sum()
            total_propostas = df_perf["qtd_propostas"].sum()

            colm1, colm2 = st.columns(2)
            with colm1:
                st.metric(
                    "Total considerado (todos digitadores)",
                    f"R$ {total_valor_considerado:,.2f}",
                )
            with colm2:
                st.metric(
                    "Total de propostas (todos digitadores)",
                    f"{total_propostas}",
                )

            # --------------------------
            # Gráficos
            # --------------------------
            st.markdown("### 📊 Gráficos")

            # Gráfico 1 – Valor considerado por digitador
            st.markdown("**Valor considerado por digitador**")
            chart_valor = df_perf.set_index("digitador")["valor_considerado"]
            st.bar_chart(chart_valor)

            # Gráfico 2 – Quantidade de propostas por digitador
            st.markdown("**Quantidade de propostas por digitador**")
            chart_qtd = df_perf.set_index("digitador")["qtd_propostas"]
            st.bar_chart(chart_qtd)

# ------------------------
# TELA 3 - USUÁRIOS
# ------------------------
elif menu == "👥 Usuários":
    # Segurança extra: só admin pode ver essa tela
    if usuario_logado["perfil"] != "admin":
        st.error("Apenas usuários com perfil **admin** podem gerenciar usuários.")
        st.stop()

    st.subheader("👥 Gestão de Usuários")

    col_form, col_lista = st.columns(2)

    # --------- Coluna de cadastro ---------
    with col_form:
        st.markdown("### ➕ Cadastrar novo usuário")
        novo_usuario = st.text_input("Usuário (login)", key="novo_usuario")
        novo_nome = st.text_input("Nome para exibição", key="novo_nome")
        novo_perfil = st.selectbox(
            "Perfil",
            ["Digitador", "Admin"],
            key="novo_perfil"
        )
        nova_senha = st.text_input("Senha", type="password", key="nova_senha")
        nova_senha2 = st.text_input("Confirmar senha", type="password", key="nova_senha2")

        if st.button("Criar usuário"):
            erros = []
            if not novo_usuario.strip():
                erros.append("Preencher o campo Usuário.")
            if not novo_nome.strip():
                erros.append("Preencher o campo Nome para exibição.")
            if not nova_senha:
                erros.append("Preencher a senha.")
            if nova_senha != nova_senha2:
                erros.append("As senhas não conferem.")

            if erros:
                st.error("⚠️ Verifique os campos:\n- " + "\n- ".join(erros))
            else:
                try:
                    criar_usuario(
                        usuario=novo_usuario.strip(),
                        nome_exibicao=novo_nome.strip(),
                        senha=nova_senha,
                        perfil=novo_perfil,
                    )
                    st.success(f"✅ Usuário '{novo_usuario}' criado com sucesso!")
                except sqlite3.IntegrityError:
                    st.error("Já existe um usuário com esse login.")
                except Exception as e:
                    st.error(f"Erro ao criar usuário: {e}")

    # --------- Coluna de listagem / exclusão ---------
    with col_lista:
        st.markdown("### 📋 Usuários cadastrados")
        df_users = listar_usuarios()

        if df_users.empty:
            st.info("Nenhum usuário cadastrado além do admin padrão.")
        else:
            st.dataframe(df_users, use_container_width=True)

            # Não permite excluir se só existe o admin
            if len(df_users) > 1:
                st.markdown("#### 🗑 Excluir usuário")
                # Filtra admin para não aparecer na lista de exclusão
                df_excluir = df_users[df_users["usuario"] != "admin"]

                if df_excluir.empty:
                    st.info("Não há outros usuários além do 'admin' para excluir.")
                else:
                    opcoes_ids = df_excluir["id"].astype(str) + " - " + df_excluir["usuario"]
                    escolha = st.selectbox(
                        "Selecione o usuário para excluir",
                        opcoes_ids
                    )
                    id_escolhido = int(escolha.split(" - ")[0])

                    if st.button("Excluir usuário selecionado"):
                        try:
                            excluir_usuario(id_escolhido)
                            st.success("Usuário excluído com sucesso.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir usuário: {e}")
            else:
                st.info("Por segurança, não é possível excluir o único usuário existente.")

# ------------------------
# TELA 4 - Logs de Auditoria
# ------------------------
elif menu == "🕒 Logs de Auditoria":
    if usuario_logado["perfil"] != "admin":
        st.error("Apenas usuários com perfil **admin** podem ver os logs de auditoria.")
        st.stop()

    st.subheader("🕒 Logs de Auditoria de Propostas")

    conn = get_connection()
    df_logs = pd.read_sql_query(
        "SELECT id, proposta_id, acao, usuario, timestamp, detalhes FROM log_propostas ORDER BY id DESC;",
        conn
    )
    conn.close()

    if df_logs.empty:
        st.info("Nenhum log registrado até o momento.")
    else:
        # converte timestamp para datetime pra filtrar por data
        df_logs["data_dt"] = pd.to_datetime(df_logs["timestamp"], errors="coerce")

        st.markdown("### 🔎 Filtros de logs")

        col_l1, col_l2, col_l3 = st.columns(3)
        with col_l1:
            filtro_acao = st.selectbox(
                "Ação",
                ["Todas", "INSERT", "UPDATE", "DELETE"],
                key="log_acao"
            )
        with col_l2:
            filtro_usuario = st.text_input(
                "Usuário (login contém)",
                key="log_usuario"
            )
        with col_l3:
            filtro_proposta = st.text_input(
                "Proposta ID (contém)",
                key="log_proposta_id"
            )

        # filtros
        df_view = df_logs.copy()

        if filtro_acao != "Todas":
            df_view = df_view[df_view["acao"] == filtro_acao]

        if filtro_usuario.strip():
            df_view = df_view[
                df_view["usuario"].str.contains(filtro_usuario.strip(), case=False, na=False)
            ]

        if filtro_proposta.strip():
            df_view = df_view[
                df_view["proposta_id"].astype(str).str.contains(filtro_proposta.strip(), na=False)
            ]

        st.markdown("### 📋 Logs registrados")
        colunas_exibir = ["id", "proposta_id", "acao", "usuario", "timestamp", "detalhes"]
        df_view = df_view[colunas_exibir].rename(columns={
            "id": "ID Log",
            "proposta_id": "ID Proposta",
            "acao": "Ação",
            "usuario": "Usuário",
            "timestamp": "Data/Hora",
            "detalhes": "Detalhes",
        })

        st.dataframe(df_view, use_container_width=True, height=500)

# ------------------------
# TELA 5 - CONSULTAS / RELATÓRIOS
# ------------------------
elif menu == "📁 Consultas / Relatórios":
    st.subheader("📁 Consultas e Relatórios")

    parceiros_opcoes = get_parceiros_opcoes()
    bancos_opcoes = get_bancos_opcoes()
    tipos_produto_lista = ["Todos", "NOVO INSS", "REFIN", "CARTÃO", "FGTS", "SAQUE COMPLEMENTAR", "NOVO - CONVENIO PUBLICO", "REFIN - CONVENIO PUBLICO", "NOVO - AUMENTO", "SEGURO DE VIDA", "CREDITO PESSOAL", "CLT"]

    df_all = carregar_propostas()

    if df_all.empty:
        st.info("Ainda não há propostas cadastradas.")
    else:
        # trata datas
        df_all["data_dt"] = pd.to_datetime(df_all["data"], errors="coerce")
        data_min = df_all["data_dt"].min()
        data_max = df_all["data_dt"].max()

        if pd.isna(data_min) or pd.isna(data_max):
            data_min = data_max = date.today()

        st.markdown("### 🔎 Filtros")

        # linha 1 de filtros
        cold1, cold2, _, _ = st.columns(4)
        with cold1:
            data_inicial = st.date_input(
                "Data inicial",
                value=data_min.date(),
                key="rep_data_ini"
            )
        with cold2:
            data_final = st.date_input(
                "Data final",
                value=data_max.date(),
                key="rep_data_fim"
            )

        # linha 2 de filtros
        colf1, colf2, colf3, colf4 = st.columns(4)

        with colf1:
            filtro_cpf = st.text_input(
                "Filtrar por CPF",
                placeholder="Digite o CPF ou parte",
                key="rep_cpf"
            )
        with colf2:
            filtro_ade = st.text_input(
                "Filtrar por ADE",
                placeholder="Digite a ADE ou parte",
                key="rep_ade"
            )
        with colf3:
            filtro_digitador = st.text_input(
                "Filtrar por Digitador",
                placeholder="Nome contém",
                key="rep_digitador"
            )
        with colf4:
            filtro_parceiro_sel = st.selectbox(
                "Filtrar por Parceiro",
                ["Todos"] + parceiros_opcoes[1:],
                key="rep_parceiro"
            )

        # linha 3 de filtros
        colb1, colb2, colb3, _ = st.columns(4)

        with colb1:
            filtro_banco_sel = st.selectbox(
                "Filtrar por Banco",
                ["Todos"] + bancos_opcoes[1:],
                key="rep_banco"
            )

        with colb2:
            filtro_id = st.text_input(
                "Filtrar por ID (opcional)",
                placeholder="Ex: 10",
                key="rep_id"
            )

        with colb3:
            filtro_tipo_produto = st.selectbox(
                "Filtrar por Tipo de Produto",
                tipos_produto_lista,
                key="rep_tipo_produto"
            )

        # aplica filtros
        df = df_all.copy()
        df["data_dt"] = pd.to_datetime(df["data"], errors="coerce")

        df = df[
            (df["data_dt"].dt.date >= data_inicial) &
            (df["data_dt"].dt.date <= data_final)
        ]

        if filtro_cpf:
            df = df[df["cpf"].str.contains(filtro_cpf, case=False, na=False)]
        if filtro_ade:
            df = df[df["ade"].str.contains(filtro_ade, case=False, na=False)]
        if filtro_digitador:
            df = df[df["digitador"].str.contains(filtro_digitador, case=False, na=False)]
        if filtro_parceiro_sel != "Todos":
            df = df[df["parceiro"] == filtro_parceiro_sel]
        if filtro_banco_sel != "Todos":
            df = df[df["banco"] == filtro_banco_sel]
        if filtro_id:
            df = df[df["id"].astype(str).str.contains(filtro_id, na=False)]
        if filtro_tipo_produto != "Todos":
            df = df[df["tipo_produto"] == filtro_tipo_produto]

        # regra de CPF com mais de uma proposta
        cpf_counts = df_all["cpf"].value_counts()
        df["ignorar_valor"] = df["cpf"].map(lambda c: cpf_counts.get(c, 0) > 1)
        df["valor_considerado"] = np.where(
            df["ignorar_valor"],
            0,
            df["valor"].fillna(0)
        )

        st.markdown("### 📋 Resultados")

        if df.empty:
            st.warning("Nenhuma proposta encontrada com os filtros informados.")
        else:
            total_valor_bruto = df["valor"].fillna(0).sum()
            total_valor_considerado = df["valor_considerado"].sum()

            colr1, colr2 = st.columns(2)
            with colr1:
                st.metric("Soma dos valores (bruto)", f"R$ {total_valor_bruto:,.2f}")
            with colr2:
                st.metric(
                    "Soma dos valores (considerando regra de CPF)",
                    f"R$ {total_valor_considerado:,.2f}"
                )

            st.caption(
                "Regra: quando um CPF tem **mais de uma proposta** na base, "
                "o sistema **ignora o campo VALOR** (coluna 'Valor Considerado' = 0) "
                "para esse cliente."
            )

            # ============================
            # EDIÇÃO / EXCLUSÃO DE PROPOSTA
            # ============================
            st.markdown("### ✏️ Editar / 🗑 Excluir proposta")

            ids_disponiveis = df["id"].sort_values().tolist()

            if not ids_disponiveis:
                st.info("Nenhuma proposta disponível para edição com os filtros atuais.")
            else:
                id_escolhido = st.selectbox(
                    "Selecione o ID da proposta para editar ou excluir",
                    ids_disponiveis,
                    format_func=lambda x: f"ID {x}"
                )

                registro = df[df["id"] == id_escolhido].iloc[0]

                with st.expander(f"Proposta selecionada (ID {id_escolhido})", expanded=True):
                    col_e1, col_e2, col_e3 = st.columns(3)

                    # Digitador / ADE / CPF
                    with col_e1:
                        digitador_edit = st.text_input(
                            "Digitador",
                            value=str(registro["digitador"] or ""),
                            key=f"edit_digitador_{id_escolhido}",
                        )
                        ade_edit = st.text_input(
                            "ADE",
                            value=str(registro["ade"] or ""),
                            key=f"edit_ade_{id_escolhido}",
                        )
                        cpf_edit = st.text_input(
                            "CPF",
                            value=str(registro["cpf"] or ""),
                            key=f"edit_cpf_{id_escolhido}",
                        )

                    # Data / Parceiro / Banco / Tipo
                    with col_e2:
                        try:
                            data_base = pd.to_datetime(registro["data"]).date()
                        except Exception:
                            data_base = date.today()

                        data_edit = st.date_input(
                            "Data da proposta",
                            value=data_base,
                            key=f"edit_data_{id_escolhido}",
                        )

                        # garante listas atualizadas
                        parceiros_opcoes = get_parceiros_opcoes()
                        bancos_opcoes = get_bancos_opcoes()
                        tipos_produto_base = ["NOVO INSS", "REFIN", "CARTÃO", "FGTS", "SAQUE COMPLEMENTAR", "NOVO - CONVENIO PUBLICO", "REFIN - CONVENIO PUBLICO", "NOVO - AUMENTO", "SEGURO DE VIDA", "CREDITO PESSOAL", "CLT"]

                        # parceiro atual da proposta
                        parceiro_val = str(registro["parceiro"] or "")
                        if parceiro_val in parceiros_opcoes:
                            idx_parceiro = parceiros_opcoes.index(parceiro_val)
                        else:
                            idx_parceiro = 0

                        parceiro_edit = st.selectbox(
                            "Parceiro",
                            parceiros_opcoes,
                            index=idx_parceiro,
                            key=f"edit_parceiro_{id_escolhido}",
                        )

                        # banco atual da proposta
                        banco_val = str(registro["banco"] or "")
                        if banco_val in bancos_opcoes:
                            idx_banco = bancos_opcoes.index(banco_val)
                        else:
                            idx_banco = 0

                        banco_edit = st.selectbox(
                            "Banco",
                            bancos_opcoes,
                            index=idx_banco,
                            key=f"edit_banco_{id_escolhido}",
                        )

                        # tipo de produto atual
                        tipo_atual = str(registro.get("tipo_produto", "") or "")
                        if tipo_atual in tipos_produto_base:
                            idx_tipo = tipos_produto_base.index(tipo_atual)
                        else:
                            idx_tipo = 0

                        tipo_produto_edit = st.selectbox(
                            "Tipo de produto",
                            tipos_produto_base,
                            index=idx_tipo,
                            key=f"edit_tipo_produto_{id_escolhido}",
                        )

                    # Valor / Botões
                    with col_e3:
                        valor_atual = (
                            registro["valor"]
                            if pd.notna(registro["valor"])
                            else ""
                        )
                        valor_edit_str = st.text_input(
                            "Valor da proposta (pode ser vazio)",
                            value=str(valor_atual),
                            key=f"edit_valor_{id_escolhido}",
                        )

                        salvar_click = st.button(
                            "💾 Salvar alterações",
                            key=f"btn_salvar_{id_escolhido}"
                        )

                        st.markdown("---")
                        st.markdown("#### 🗑 Excluir proposta")
                        confirma_excluir = st.checkbox(
                            "Confirmo que desejo excluir esta proposta",
                            key=f"chk_excluir_{id_escolhido}",
                        )
                        excluir_click = st.button(
                            "Excluir proposta",
                            key=f"btn_excluir_{id_escolhido}"
                        )

                    # --- Ações dos botões ---
                    if salvar_click:
                        erros_edit = []
                        if not ade_edit.strip():
                            erros_edit.append("Preencher o campo ADE.")
                        if not cpf_edit.strip():
                            erros_edit.append("Preencher o campo CPF.")
                        if parceiro_edit == "Selecione o parceiro":
                            erros_edit.append("Selecionar um parceiro.")
                        if banco_edit == "Selecione o banco":
                            erros_edit.append("Selecionar um banco.")

                        valor_num = None
                        if valor_edit_str.strip() != "":
                            try:
                                valor_num = float(
                                    valor_edit_str.replace(".", "").replace(",", ".")
                                )
                            except ValueError:
                                erros_edit.append(
                                    "Valor inválido. Use apenas números, vírgula ou ponto."
                                )

                        if erros_edit:
                            st.error("⚠️ Verifique os campos:\n- " + "\n- ".join(erros_edit))
                        else:
                            try:
                                # monta snapshot antes
                                detalhes_antes = (
                                    f"ANTES: digitador={registro['digitador']}, "
                                    f"ade={registro['ade']}, cpf={registro['cpf']}, data={registro['data']}, "
                                    f"parceiro={registro['parceiro']}, tipo_produto={registro.get('tipo_produto','')}, "
                                    f"valor={registro['valor']}, banco={registro['banco']}"
                                )

                                atualizar_proposta(
                                    id_proposta=id_escolhido,
                                    digitador=digitador_edit.strip(),
                                    ade=ade_edit.strip(),
                                    cpf=cpf_edit.strip(),
                                    data_str=data_edit.strftime("%Y-%m-%d"),
                                    parceiro=parceiro_edit.strip(),
                                    tipo_produto=tipo_produto_edit,
                                    valor=valor_num,
                                    banco=banco_edit.strip(),
                                )

                                # snapshot depois
                                detalhes_depois = (
                                    f"DEPOIS: digitador={digitador_edit.strip()}, "
                                    f"ade={ade_edit.strip()}, cpf={cpf_edit.strip()}, data={data_edit.strftime('%Y-%m-%d')}, "
                                    f"parceiro={parceiro_edit.strip()}, tipo_produto={tipo_produto_edit}, "
                                    f"valor={valor_num}, banco={banco_edit.strip()}"
                                )

                                try:
                                    registrar_log_proposta(
                                        acao="UPDATE",
                                        usuario=usuario_logado["usuario"],
                                        proposta_id=id_escolhido,
                                        detalhes=f"{detalhes_antes} | {detalhes_depois}",
                                    )
                                except Exception as e_log:
                                    st.warning(f"Atualizou, mas não conseguiu registrar log: {e_log}")

                                st.success("✅ Proposta atualizada com sucesso!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Erro ao atualizar proposta: {e}")

                if excluir_click:
                    if not confirma_excluir:
                        st.warning("Marque a caixa de confirmação antes de excluir.")
                    else:
                        try:
                            # snapshot antes de excluir
                            detalhes_antes = (
                                f"EXCLUINDO: digitador={registro['digitador']}, "
                                f"ade={registro['ade']}, cpf={registro['cpf']}, data={registro['data']}, "
                                f"parceiro={registro['parceiro']}, tipo_produto={registro.get('tipo_produto','')}, "
                                f"valor={registro['valor']}, banco={registro['banco']}"
                            )

                            excluir_proposta_bd(id_escolhido)

                            try:
                                registrar_log_proposta(
                                    acao="DELETE",
                                    usuario=usuario_logado["usuario"],
                                    proposta_id=id_escolhido,
                                    detalhes=detalhes_antes,
                                )
                            except Exception as e_log:
                                st.warning(f"Excluiu, mas não conseguiu registrar log: {e_log}")

                            st.success("🗑 Proposta excluída com sucesso!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao excluir proposta: {e}")

            # --------- TABELA FINAL ---------
            colunas_ordem = [
                "id", "digitador", "ade", "cpf", "data",
                "parceiro", "tipo_produto", "banco", "valor",
                "valor_considerado", "ignorar_valor"
            ]
            colunas_existentes = [c for c in colunas_ordem if c in df.columns]
            df_exibe = df[colunas_existentes].copy()

            df_exibe = df_exibe.rename(columns={
                "id": "ID",
                "digitador": "Digitador",
                "ade": "ADE",
                "cpf": "CPF",
                "data": "Data",
                "parceiro": "Parceiro",
                "tipo_produto": "Tipo de Produto",
                "banco": "Banco",
                "valor": "Valor",
                "valor_considerado": "Valor Considerado",
                "ignorar_valor": "Ignorar Valor?"
            })

            st.dataframe(df_exibe, use_container_width=True)

            # -------------------------------------------------
            # 📥 Exportar dados filtrados (Excel e CSV)
            # -------------------------------------------------
            st.markdown("### 📥 Exportar dados filtrados")

            col_exp1, col_exp2 = st.columns(2)

            # 🔹 Exportar para Excel (.xlsx)
            with col_exp1:
                try:
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                        df_exibe.to_excel(writer, index=False, sheet_name="Propostas")
                    st.download_button(
                        label="⬇️ Baixar Excel (.xlsx)",
                        data=buffer.getvalue(),
                        file_name="propostas_filtradas.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_export_excel",
                    )
                except Exception as e:
                    st.error(f"Erro ao exportar para Excel: {e}")

            # 🔹 Exportar para CSV (.csv)
            with col_exp2:
                try:
                    csv = df_exibe.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        label="⬇️ Baixar CSV (.csv)",
                        data=csv,
                        file_name="propostas_filtradas.csv",
                        mime="text/csv",
                        key="btn_export_csv",
                    )
                except Exception as e:
                    st.error(f"Erro ao exportar para CSV: {e}")

# ------------------------
# TELA 6 - Cadastro de Parceiros
# ------------------------
elif menu == "🤝 Cadastro de Parceiros":
    if usuario_logado["perfil"] != "admin":
        st.error("Apenas usuários com perfil **admin** podem gerenciar parceiros.")
        st.stop()

    st.subheader("🤝 Cadastro de Parceiros")

    col_p1, col_p2 = st.columns(2)

    with col_p1:
        st.markdown("### ➕ Novo parceiro")
        nova_desc_parc = st.text_input(
            "Descrição do parceiro (exatamente como quer ver nos selects)",
            key="novo_parceiro_desc",
        )
        if st.button("Adicionar parceiro"):
            if not nova_desc_parc.strip():
                st.error("Informe a descrição do parceiro.")
            else:
                try:
                    inserir_parceiro(nova_desc_parc)
                    st.success("Parceiro cadastrado com sucesso!")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("Já existe um parceiro com essa descrição.")
                except Exception as e:
                    st.error(f"Erro ao cadastrar parceiro: {e}")

    with col_p2:
        st.markdown("### 📋 Parceiros cadastrados")
        df_parc = listar_parceiros_bd()
        if df_parc.empty:
            st.info("Nenhum parceiro cadastrado.")
        else:
            df_view = df_parc.rename(columns={
                "id": "ID",
                "descricao": "Descrição",
                "ativo": "Ativo",
            })
            st.dataframe(df_view, use_container_width=True, height=400)

            st.markdown("#### 🔧 Alterar status / Excluir")
            opcoes_parc = df_parc["id"].astype(str) + " - " + df_parc["descricao"]
            escolha_parc = st.selectbox(
                "Selecione o parceiro",
                opcoes_parc,
                key="parc_sel_acao",
            )
            parc_id = int(escolha_parc.split(" - ")[0])

            acao_parc = st.radio(
                "Ação",
                ["Ativar", "Desativar", "Excluir"],
                key="parc_radio_acao",
                horizontal=True,
            )

            if st.button("Aplicar ação no parceiro selecionado"):
                try:
                    if acao_parc == "Ativar":
                        alterar_status_parceiro(parc_id, 1)
                    elif acao_parc == "Desativar":
                        alterar_status_parceiro(parc_id, 0)
                    else:
                        excluir_parceiro(parc_id)
                    st.success("Ação aplicada com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao aplicar ação: {e}")

# ------------------------
# TELA 7 - Cadastro de Bancos
# ------------------------
elif menu == "🏦 Cadastro de Bancos":
    if usuario_logado["perfil"] != "admin":
        st.error("Apenas usuários com perfil **admin** podem gerenciar bancos.")
        st.stop()

    st.subheader("🏦 Cadastro de Bancos")

    col_b1, col_b2 = st.columns(2)

    with col_b1:
        st.markdown("### ➕ Novo banco")
        nova_desc_banco = st.text_input(
            "Descrição do banco (exatamente como quer ver nos selects)",
            key="novo_banco_desc",
        )
        if st.button("Adicionar banco"):
            if not nova_desc_banco.strip():
                st.error("Informe a descrição do banco.")
            else:
                try:
                    inserir_banco(nova_desc_banco)
                    st.success("Banco cadastrado com sucesso!")
                    st.rerun()
                except sqlite3.IntegrityError:
                    st.error("Já existe um banco com essa descrição.")
                except Exception as e:
                    st.error(f"Erro ao cadastrar banco: {e}")

    with col_b2:
        st.markdown("### 📋 Bancos cadastrados")
        df_ban = listar_bancos_bd()
        if df_ban.empty:
            st.info("Nenhum banco cadastrado.")
        else:
            df_view_b = df_ban.rename(columns={
                "id": "ID",
                "descricao": "Descrição",
                "ativo": "Ativo",
            })
            st.dataframe(df_view_b, use_container_width=True, height=400)

            st.markdown("#### 🔧 Alterar status / Excluir")
            opcoes_ban = df_ban["id"].astype(str) + " - " + df_ban["descricao"]
            escolha_ban = st.selectbox(
                "Selecione o banco",
                opcoes_ban,
                key="ban_sel_acao",
            )
            banco_id = int(escolha_ban.split(" - ")[0])

            acao_ban = st.radio(
                "Ação",
                ["Ativar", "Desativar", "Excluir"],
                key="ban_radio_acao",
                horizontal=True,
            )

            if st.button("Aplicar ação no banco selecionado"):
                try:
                    if acao_ban == "Ativar":
                        alterar_status_banco(banco_id, 1)
                    elif acao_ban == "Desativar":
                        alterar_status_banco(banco_id, 0)
                    else:
                        excluir_banco(banco_id)
                    st.success("Ação aplicada com sucesso.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao aplicar ação: {e}")

# ================================
# RODAPÉ FIXO (INFORMAÇÕES DO SISTEMA)
# ================================

agora = datetime.now().strftime("%a, %d %b de %Y %H:%M:%S") \
    .replace("Mon", "seg") \
    .replace("Tue", "ter") \
    .replace("Wed", "qua") \
    .replace("Thu", "qui") \
    .replace("Fri", "sex") \
    .replace("Sat", "sab") \
    .replace("Sun", "dom") \
    .replace("Jan", "jan") \
    .replace("Feb", "fev") \
    .replace("Mar", "mar") \
    .replace("Apr", "abr") \
    .replace("May", "mai") \
    .replace("Jun", "jun") \
    .replace("Jul", "jul") \
    .replace("Aug", "ago") \
    .replace("Sep", "set") \
    .replace("Oct", "out") \
    .replace("Nov", "nov") \
    .replace("Dec", "dez")

rodape_html = f"""
<style>
.footer {{
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    color: #888;
    text-align: center;
    font-size: 14px;
    padding: 6px 0 4px 0;
    background-color: rgba(0,0,0,0.02);
}}
</style>
<div class="footer">
    © 2014–2025 <b>Grupo Evolve</b>.
    versão: <b>v{version_info['version']}</b> (build {version_info['build']})
    — {version_info['generatedAt']} · 👤 {digitador_logado} · 🌐 IP do login: {ip_usuario}
</div>
"""

st.markdown(rodape_html, unsafe_allow_html=True)
