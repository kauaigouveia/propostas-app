import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import sqlite3

# ------------------------
# CONFIGURAÇÕES INICIAIS
# ------------------------
st.set_page_config(
    page_title="Controle de Propostas",
    layout="wide"
)

# ------------------------
# LISTAS DE OPÇÕES
# ------------------------

DIGITADORES_OPCOES = [
    "Selecione o digitador",
    "Kauai",
    "Andressa",
    "Dani",
    "Jocyane",
]

BANCOS_OPCOES = [
    "Selecione o banco",

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

PARCEIROS_OPCOES = [
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

# ------------------------
# Funções de banco de dados
# ------------------------
def get_connection():
    return sqlite3.connect("propostas.db", check_same_thread=False)


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS propostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            digitador TEXT NOT NULL,
            ade TEXT NOT NULL,
            cpf TEXT NOT NULL,
            data TEXT NOT NULL,
            parceiro TEXT NOT NULL,
            valor REAL,
            banco TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()


def inserir_proposta(digitador, ade, cpf, data_str, parceiro, valor, banco):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO propostas (digitador, ade, cpf, data, parceiro, valor, banco)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (digitador, ade, cpf, data_str, parceiro, valor, banco),
    )
    conn.commit()
    conn.close()


def carregar_propostas():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM propostas ORDER BY id DESC;", conn)
    conn.close()
    return df


# Inicializa o banco
init_db()

# ------------------------
# SIDEBAR - LOGIN E MENU
# ------------------------
with st.sidebar:
    st.markdown("### 👤 Digitador logado")
    digitador_logado = st.selectbox(
        "Selecione o digitador",
        DIGITADORES_OPCOES,
        index=0,
        label_visibility="collapsed"
    )
    st.write("")

    menu = st.radio(
        "Menu",
        ["📄 Lançamento de Propostas", "📊 Dashboard", "📁 Consultas / Relatórios"]
    )

# ------------------------
# TELA 1 - LANÇAMENTO DE PROPOSTAS
# ------------------------
if menu == "📄 Lançamento de Propostas":
    st.subheader("📝 Lançamento de Propostas")

    if digitador_logado == "Selecione o digitador":
        st.warning("Selecione o **digitador logado** no menu à esquerda antes de lançar propostas.")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.text_input(
            "Digitador (do login)",
            value=digitador_logado if digitador_logado != "Selecione o digitador" else "",
            disabled=True
        )
        ade = st.text_input("ADE", placeholder="Número da ADE")
        cpf = st.text_input("CPF do cliente", placeholder="Somente números")

    with col2:
        data_proposta = st.date_input("Data da proposta", value=date.today())

        parceiro = st.selectbox(
            "Parceiro",
            PARCEIROS_OPCOES,
            index=0
        )

        banco = st.selectbox(
            "Banco",
            BANCOS_OPCOES,
            index=0
        )

    with col3:
        valor_str = st.text_input(
            "Valor da proposta",
            placeholder="Ex: 1500,00 (pode usar vírgula ou ponto)"
        )
        valor = None
        if valor_str.strip() != "":
            try:
                valor = float(valor_str.replace(".", "").replace(",", "."))
            except ValueError:
                st.warning("⚠️ Valor inválido. Use apenas números, vírgula e ponto.")

    if st.button("Salvar proposta"):
        erros = []

        if digitador_logado == "Selecione o digitador":
            erros.append("Selecionar o digitador logado (menu à esquerda).")
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
            data_str = data_proposta.strftime("%Y-%m-%d")
            valor_num = valor if valor is not None else None

            inserir_proposta(
                digitador=digitador_logado.strip(),
                ade=ade.strip(),
                cpf=cpf.strip(),
                data_str=data_str,
                parceiro=parceiro.strip(),
                valor=valor_num,
                banco=banco.strip(),
            )

            st.success("✅ Proposta salva com sucesso!")
            st.info(
                "Você pode lançar várias propostas com o mesmo CPF e ADEs diferentes.\n"
                "No cálculo de valores, quando o CPF tiver mais de uma proposta, o sistema ignora o campo VALOR."
            )

# ------------------------
# TELA 2 - DASHBOARD (INDICADORES + GRÁFICOS)
# ------------------------
elif menu == "📊 Dashboard":
    st.subheader("📊 Dashboard")

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

        st.markdown("### 🔎 Filtros (Dashboard)")

        # Filtros principais
        cold1, cold2, _, _ = st.columns(4)
        with cold1:
            data_inicial = st.date_input("Data inicial", value=data_min.date(), key="dash_data_ini")
        with cold2:
            data_final = st.date_input("Data final", value=data_max.date(), key="dash_data_fim")

        colf1, colf2, colf3, colf4 = st.columns(4)
        with colf1:
            filtro_digitador = st.selectbox(
                "Digitador",
                ["Todos"] + DIGITADORES_OPCOES[1:],
                key="dash_digitador"
            )
        with colf2:
            filtro_parceiro_sel = st.selectbox(
                "Parceiro",
                ["Todos"] + PARCEIROS_OPCOES[1:],
                key="dash_parceiro"
            )
        with colf3:
            filtro_banco_sel = st.selectbox(
                "Banco",
                ["Todos"] + BANCOS_OPCOES[1:],
                key="dash_banco"
            )
        with colf4:
            filtro_cpf = st.text_input("CPF (opcional)", placeholder="Filtrar por CPF", key="dash_cpf")

        # Aplica filtros
        df = df_all.copy()
        df["data_dt"] = pd.to_datetime(df["data"], errors="coerce")

        df = df[
            (df["data_dt"].dt.date >= data_inicial) &
            (df["data_dt"].dt.date <= data_final)
        ]

        if filtro_digitador != "Todos":
            df = df[df["digitador"] == filtro_digitador]
        if filtro_parceiro_sel != "Todos":
            df = df[df["parceiro"] == filtro_parceiro_sel]
        if filtro_banco_sel != "Todos":
            df = df[df["banco"] == filtro_banco_sel]
        if filtro_cpf:
            df = df[df["cpf"].str.contains(filtro_cpf, case=False, na=False)]

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
                st.metric("Soma dos valores (bruto)", f"R$ {total_valor_bruto:,.2f}")
            with colr2:
                st.metric("Soma dos valores (considerando regra de CPF)", f"R$ {total_valor_considerado:,.2f}")

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

            st.caption(
                "Regra aplicada no dashboard: quando um CPF tem **mais de uma proposta** na base, "
                "o sistema **ignora o campo VALOR** (coluna 'Valor Considerado' = 0) para esse cliente."
            )

# ------------------------
# TELA 3 - CONSULTAS / RELATÓRIOS (TABELA + FILTROS COMPLETOS)
# ------------------------
elif menu == "📁 Consultas / Relatórios":
    st.subheader("📁 Consultas e Relatórios")

    df_all = carregar_propostas()

    if df_all.empty:
        st.info("Ainda não há propostas cadastradas.")
    else:
        df_all["data_dt"] = pd.to_datetime(df_all["data"], errors="coerce")
        data_min = df_all["data_dt"].min()
        data_max = df_all["data_dt"].max()

        if pd.isna(data_min) or pd.isna(data_max):
            data_min = data_max = date.today()

        st.markdown("### 🔎 Filtros")

        cold1, cold2, _, _ = st.columns(4)
        with cold1:
            data_inicial = st.date_input("Data inicial", value=data_min.date(), key="rep_data_ini")
        with cold2:
            data_final = st.date_input("Data final", value=data_max.date(), key="rep_data_fim")

        colf1, colf2, colf3, colf4 = st.columns(4)
        with colf1:
            filtro_cpf = st.text_input("Filtrar por CPF", placeholder="Digite o CPF ou parte", key="rep_cpf")
        with colf2:
            filtro_ade = st.text_input("Filtrar por ADE", placeholder="Digite a ADE ou parte", key="rep_ade")
        with colf3:
            filtro_digitador = st.selectbox(
                "Filtrar por Digitador",
                ["Todos"] + DIGITADORES_OPCOES[1:],
                key="rep_digitador"
            )
        with colf4:
            filtro_parceiro_sel = st.selectbox(
                "Filtrar por Parceiro",
                ["Todos"] + PARCEIROS_OPCOES[1:],
                key="rep_parceiro"
            )

        colb1, colb2, _, _ = st.columns(4)
        with colb1:
            filtro_banco_sel = st.selectbox(
                "Filtrar por Banco",
                ["Todos"] + BANCOS_OPCOES[1:],
                key="rep_banco"
            )
        with colb2:
            filtro_id = st.text_input("Filtrar por ID (opcional)", placeholder="Ex: 10", key="rep_id")

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
        if filtro_digitador != "Todos":
            df = df[df["digitador"] == filtro_digitador]
        if filtro_parceiro_sel != "Todos":
            df = df[df["parceiro"] == filtro_parceiro_sel]
        if filtro_banco_sel != "Todos":
            df = df[df["banco"] == filtro_banco_sel]
        if filtro_id:
            df = df[df["id"].astype(str).str.contains(filtro_id, na=False)]

        # mesma regra de CPF
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
                st.metric("Soma dos valores (considerando regra de CPF)", f"R$ {total_valor_considerado:,.2f}")

            st.caption(
                "Regra: quando um CPF tem **mais de uma proposta** na base, "
                "o sistema **ignora o campo VALOR** (coluna 'Valor Considerado' = 0) "
                "para esse cliente."
            )

            colunas_ordem = [
                "id", "digitador", "ade", "cpf", "data",
                "parceiro", "banco", "valor", "valor_considerado", "ignorar_valor"
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
                "banco": "Banco",
                "valor": "Valor",
                "valor_considerado": "Valor Considerado",
                "ignorar_valor": "Ignorar Valor?"
            })

            st.dataframe(df_exibe, use_container_width=True)