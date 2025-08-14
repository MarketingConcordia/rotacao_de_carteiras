import streamlit as st
import os
import tempfile
import pyodbc
import sqlite3
import zipfile
import numpy as np
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

import warnings
warnings.filterwarnings('ignore')

# ---------- FUNÇÕES DE BANCO DE DADOS ----------

# Função para buscar vendedores do banco de dados principal (SQL Server)
def get_vendedores_from_sql_server():
    server = st.secrets["DB_SERVER"]
    database = st.secrets["DB_NAME"]
    username = st.secrets["DB_USER"]
    password = st.secrets["DB_PASSWORD"]

    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(connection_string)

    query = """
    SELECT 
        razao_social 
    FROM 
        dbo.pessoas
    WHERE
        vendedor = 1
        AND ativo = 1;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df['razao_social'].tolist()

def connect_db():
    conn = sqlite3.connect("vendedores.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendedores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            tipo TEXT NOT NULL CHECK(tipo IN ('Distribuição', 'Corporativo', 'Outro Tipo'))
        )
    """)
    conn.commit()

    try:
        vendedores_do_sql_server = get_vendedores_from_sql_server()
        
        for nome_vendedor in vendedores_do_sql_server:
            # Insere com tipo padrão "Distribuição", mas pode ser alterado na UI
            tipo_padrao = 'Distribuição' 
            cursor.execute("INSERT OR IGNORE INTO vendedores (nome, tipo) VALUES (?, ?)", (nome_vendedor, tipo_padrao))
        
        conn.commit()
    except Exception as e:
        st.error(f"Erro ao carregar vendedores do SQL Server: {e}")

    return conn

def carregar_vendedores():
    conn = sqlite3.connect("vendedores.db")
    df = pd.read_sql("SELECT nome, tipo FROM vendedores", conn)
    return df

def criar_tabela_historico():
    conn = sqlite3.connect('historico_rotacao.db')
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS historico_rotacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_vendedor TEXT,
        conta_id INTEGER,
        tipo_rotacao TEXT,
        data_rotacao TEXT
    )
    ''')
    conn.commit()
    conn.close()

criar_tabela_historico()

# ---------- CONFIGURAÇÕES INICIAIS ----------
st.set_page_config(page_title="Rotação de Carteiras", layout="wide")
st.title("🔁 Sistema de Rotação de Carteiras")

# ---------- CONEXÃO COM BANCO DE DADOS PRINCIPAL ----------
@st.cache_data
def carregar_dados_sql():
    server = st.secrets["DB_SERVER"]
    database = st.secrets["DB_NAME"]
    username = st.secrets["DB_USER"]
    password = st.secrets["DB_PASSWORD"]

    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )

    conn = pyodbc.connect(connection_string)

    query = """
-- Seção de CTEs
WITH Faturamento AS (
    SELECT 
        pessoa_id, 
        SUM(valor_total) AS valor_total
    FROM 
        dbo.rel_faturamento
    WHERE 
        data_emissao >= DATEADD(MONTH, -6, GETDATE())
    GROUP BY 
        pessoa_id
),
Followups AS (
    SELECT 
        pessoa_id, 
        COUNT(*) AS total_followups, 
        MAX(data_cadastro) AS data_ultimo_followup
    FROM 
        dbo.pessoas_followup_anexos
    GROUP BY 
        pessoa_id
),
Contatos AS (
    SELECT 
        pessoa_id, 
        COUNT(*) AS total_contatos, 
        MAX(data_cadastro) AS data_ultimo_contato
    FROM 
        dbo.contatos
    GROUP BY 
        pessoa_id
),
Oportunidades AS (
    SELECT 
        conta_id AS pessoa_id, 
        COUNT(*) AS total_oportunidades, 
        MAX(data_cadastro) AS data_ultima_oportunidade
    FROM 
        dbo.crm_oportunidades
    GROUP BY 
        conta_id
),
UltimaVendaPorRaizCNPJ AS (
    SELECT 
        LEFT(cpf_cnpj, 8) AS Raiz_CNPJ, 
        MAX(data_ultima_venda) AS Data_Ultima_Venda_Grupo_CNPJ
    FROM 
        dbo.pessoas
    WHERE 
        data_ultima_venda IS NOT NULL
    GROUP BY 
        LEFT(cpf_cnpj, 8)
),
Pedidos AS (
    SELECT 
        pessoa_id,
        COUNT(*) AS total_pedidos
    FROM 
        dbo.rel_faturamento
    WHERE 
        data_emissao >= DATEADD(MONTH, -6, GETDATE())
    GROUP BY 
        pessoa_id
),
Orcamentos AS (
    SELECT 
        pessoa_cliente_id,
        COUNT(*) AS total_orcamentos,
        MAX(data_emissao) AS data_ultimo_orcamento
    FROM 
        dbo.rel_crm_orcamentos
    GROUP BY 
        pessoa_cliente_id
),
PedidosPorRevenda AS (
    SELECT 
        p.revenda_id AS pessoa_id,
        COUNT(*) AS total_pedidos_revenda,
        SUM(p.valor_total) AS valor_total_revenda,
        MAX(p.data_faturamento) AS ultima_data_pedido_revenda
    FROM 
        dbo.rel_pedidos p
    WHERE 
        p.revenda_id IS NOT NULL
        AND p.data_faturamento >= DATEADD(MONTH, -6, GETDATE())
    GROUP BY 
        p.revenda_id
)
-- Query principal
SELECT 
    a.id AS Conta_ID,
    a.tipo_conta,
    b.razao_social AS Razao_Social_Pessoas,
    b.cpf_cnpj AS CNPJ,
    LEFT(b.cpf_cnpj, 8) AS Raiz_CNPJ,
    c.grupo_id AS Grupo_Econômico_ID,
    c.grupo_nome AS Grupo_Econômico_Nome,
    v.razao_social AS Nome_Vendedor,
    b.data_ultima_venda AS Data_Ultima_Venda_Individual,

    COALESCE(f.valor_total, 0) + COALESCE(pr.valor_total_revenda, 0) AS Faturamento_6_Meses,

    a.data_cadastro AS Data_Abertura_Conta,
    COALESCE(p.total_pedidos, 0) + COALESCE(pr.total_pedidos_revenda, 0) AS Total_Pedidos,
    COALESCE(g.Data_Ultima_Venda_Grupo_CNPJ, b.data_ultima_venda) AS Data_Ultima_Venda_Grupo_CNPJ,
    COALESCE(fu.total_followups, 0) AS Total_Followups,
    fu.data_ultimo_followup AS Data_Ultimo_Followup,
    COALESCE(ct.total_contatos, 0) AS Total_Contatos,
    ct.data_ultimo_contato AS Data_Ultimo_Contato,
    COALESCE(o.total_oportunidades, 0) AS Total_Oportunidades,
    o.data_ultima_oportunidade AS Data_Ultima_Oportunidade,
    a.classificacao_id AS Classificacao_Conta,
    b.classificacao_id AS Classificacao_Pessoa,
    a.porte_id AS Porte_Empresa,

    (
        SELECT COUNT(*) 
        FROM dbo.rel_crm_orcamentos d
        WHERE d.pessoa_cliente_id = b.id
    ) AS Total_Orcamentos,

    (
        SELECT MAX(data_emissao)
        FROM dbo.rel_crm_orcamentos d
        WHERE d.pessoa_cliente_id = b.id
    ) AS Data_Ultimo_Orcamento

FROM
    grupofort.dbo.crm_contas a
    INNER JOIN dbo.pessoas b ON a.cliente_id = b.id
    INNER JOIN dbo.rel_pessoas c ON b.id = c.id
    INNER JOIN dbo.pessoas v ON a.vendedor_id = v.id
    LEFT JOIN Faturamento f ON a.cliente_id = f.pessoa_id
    LEFT JOIN Followups fu ON b.id = fu.pessoa_id
    LEFT JOIN Contatos ct ON b.id = ct.pessoa_id
    LEFT JOIN Oportunidades o ON a.id = o.pessoa_id
    LEFT JOIN UltimaVendaPorRaizCNPJ g ON LEFT(b.cpf_cnpj, 8) = g.Raiz_CNPJ
    LEFT JOIN Pedidos p ON a.cliente_id = p.pessoa_id
    LEFT JOIN PedidosPorRevenda pr ON b.id = pr.pessoa_id

WHERE
    a.tipo_conta = 2
    AND a.excluido = 0
    AND a.status_conta = 0
    AND b.classificacao_id <> 1
    AND a.classificacao_id <> 1;
"""
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ---------- SELEÇÃO DE GRUPO DE VENDEDORES ----------

conn = connect_db()
cursor = conn.cursor()
df_vendedores = carregar_vendedores()

st.markdown("#### 🛠️ **Gerencie o cadastro de seus vendedores ⬇️**")
with st.expander("Clique aqui para expandir"):
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ➕ Cadastrar vendedor")
        nomes_vendedores_empresa = sorted(get_vendedores_from_sql_server())

        # Adiciona opções extras
        opcoes = [""] + nomes_vendedores_empresa + ["Outro (digitar manualmente)"]

        nome = st.selectbox(
            "Digite ou selecione o nome do vendedor",
            options=opcoes,
            index=0,  # começa vazio
            placeholder="Busque ou digite o nome..."
        )

        # Se escolher "Outro", mostrar campo manual
        if nome == "Outro (digitar manualmente)":
            nome = st.text_input("Digite o nome manualmente")
        tipo = st.selectbox("Tipo", ["Distribuição", "Corporativo", "Outro Tipo"])
        if st.button("Cadastrar vendedor"):
            if nome.strip() == "":
                st.warning("Digite um nome válido.")
            elif nome in df_vendedores["nome"].values:
                st.warning("Esse nome já está cadastrado.")
            else:
                cursor.execute("INSERT INTO vendedores (nome, tipo) VALUES (?, ?)", (nome.strip(), tipo))
                conn.commit()
                st.success(f"{nome} adicionado com sucesso!")
                st.rerun()

    with col2:
        st.markdown("### Lista de vendedores")

        distribuidores = df_vendedores[df_vendedores["tipo"] == "Distribuição"]["nome"].tolist()
        corporativos = df_vendedores[df_vendedores["tipo"] == "Corporativo"]["nome"].tolist()
        outros_tipos = df_vendedores[df_vendedores["tipo"] == "Outro Tipo"]["nome"].tolist()

        def remover_vendedor(nome):
            cursor.execute("DELETE FROM vendedores WHERE nome = ?", (nome,))
            conn.commit()
            st.success(f"Vendedor '{nome}' removido com sucesso.")
            st.rerun()

        if "confirma_remocao" not in st.session_state:
            st.session_state.confirma_remocao = None

        def pedir_confirmacao(nome):
            st.session_state.confirma_remocao = nome

        def cancelar_remocao():
            st.session_state.confirma_remocao = None

        st.markdown("#### Distribuição")
        if distribuidores:
            for nome in distribuidores:
                if st.session_state.confirma_remocao == nome:
                    st.warning(f"Tem certeza que deseja remover **{nome}**?")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("✅ Confirmar remoção", key=f"confirmar_dist_{nome}"):
                            remover_vendedor(nome)
                    with col_b:
                        if st.button("❌ Cancelar", key=f"cancelar_dist_{nome}"):
                            cancelar_remocao()
                else:
                    col1, col2 = st.columns([8,1])
                    with col1:
                        st.write(nome)
                    with col2:
                        st.button(f"❌", key=f"remover_dist_{nome}", on_click=pedir_confirmacao, args=(nome,))
        else:
            st.write("_Nenhum vendedor cadastrado._")

        st.markdown("#### Corporativo")
        if corporativos:
            for nome in corporativos:
                if st.session_state.confirma_remocao == nome:
                    st.warning(f"Tem certeza que deseja remover **{nome}**?")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("✅ Confirmar remoção", key=f"confirmar_corp_{nome}"):
                            remover_vendedor(nome)
                    with col_b:
                        if st.button("❌ Cancelar", key=f"cancelar_corp_{nome}"):
                            cancelar_remocao()
                else:
                    col1, col2 = st.columns([8,1])
                    with col1:
                        st.write(nome)
                    with col2:
                        st.button(f"❌", key=f"remover_corp_{nome}", on_click=pedir_confirmacao, args=(nome,))
        else:
            st.write("_Nenhum vendedor cadastrado._")

        st.markdown("#### Outro Tipo")
        if outros_tipos:
            for nome in outros_tipos:
                if st.session_state.confirma_remocao == nome:
                    st.warning(f"Tem certeza que deseja remover **{nome}**?")
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("✅ Confirmar remoção", key=f"confirmar_outros_{nome}"):
                            remover_vendedor(nome)
                    with col_b:
                        if st.button("❌ Cancelar", key=f"cancelar_outros_{nome}"):
                            cancelar_remocao()
                else:
                    col1, col2 = st.columns([8,1])
                    with col1:
                        st.write(nome)
                    with col2:
                        st.button(f"❌", key=f"remover_outros_{nome}", on_click=pedir_confirmacao, args=(nome,))
        else:
            st.write("_Nenhum vendedor cadastrado._")

conn = connect_db()
cursor = conn.cursor()

query = "SELECT nome FROM vendedores WHERE tipo = ?"
cursor.execute(query, ('Distribuição',))
vendedores_ativos_helder = [row[0] for row in cursor.fetchall()]

query = "SELECT nome FROM vendedores WHERE tipo = ?"
cursor.execute(query, ('Corporativo',))
vendedores_ativos_karen = [row[0] for row in cursor.fetchall()]

query = "SELECT nome FROM vendedores WHERE tipo = ?"
cursor.execute(query, ('Outro Tipo',))
vendedores_ativos_outro = [row[0] for row in cursor.fetchall()]

opcao = st.selectbox(
    "Escolha o grupo de vendedores:", 
    ["Distribuição (Helder)", "Corporativo (Karen)", "Outro Tipo"]
)

if "Helder" in opcao:
    vendedores_ativos = vendedores_ativos_helder
    pasta_relatorios = 'Relatorio_Vendedores_Helder'
elif "Karen" in opcao:
    vendedores_ativos = vendedores_ativos_karen
    pasta_relatorios = 'Relatorio_Vendedores_Karen'
else:
    vendedores_ativos = vendedores_ativos_outro
    pasta_relatorios = 'Relatorio_Vendedores_Outro'

st.markdown('------')
# ---------- LEITURA DA REFERÊNCIA ----------
st.markdown('#### 1-Faça o upload do arquivo: historico de rotação com a data mais recente ☁️')
arquivo_referencia = st.file_uploader("📤 Clique em 'Drag and Drop' ou 'Browse files', selecione o arquivo com a data mais recente e envie o arquivo (histórico de rotação de carteiras):", type=["xlsx"])

if arquivo_referencia:
    df = carregar_dados_sql()
    df = df.drop_duplicates(subset='Raiz_CNPJ')
    referencia = pd.read_excel(arquivo_referencia, sheet_name='Planilha1')

    # --- Carregar data de rotação por conta_id ---
    conn = sqlite3.connect('historico_rotacao.db')
    df_rotacao = pd.read_sql_query('''
        SELECT conta_id, MAX(data_rotacao) as data_ultima_rotacao 
        FROM historico_rotacao 
        GROUP BY conta_id
    ''', conn)
    conn.close()

    df_rotacao['data_ultima_rotacao'] = pd.to_datetime(df_rotacao['data_ultima_rotacao'])

    df_rotacao['conta_id'] = df_rotacao['conta_id'].apply(
    lambda x: int.from_bytes(x, byteorder='little') if isinstance(x, bytes) else int(x)
    )

    df['Raiz_CNPJ'] = df['Raiz_CNPJ'].astype(str).str.strip().str.zfill(14)
    referencia['Raiz_CNPJ'] = referencia['Raiz_CNPJ'].astype(str).str.strip().str.zfill(14)
    dict_transferencia = dict(zip(referencia['Raiz_CNPJ'], referencia['Nome_Vendedor']))

    # Adiciona a coluna 'data_ultima_rotacao' com base no Conta_ID
    df = df.merge(df_rotacao, how='left', left_on='Conta_ID', right_on='conta_id')

    # Atualiza o Nome_Vendedor do df conforme a referência
    df['Nome_Vendedor'] = df.apply(
        lambda row: dict_transferencia[row['Raiz_CNPJ']] if row['Raiz_CNPJ'] in dict_transferencia else row['Nome_Vendedor'],
        axis=1
    )


    # Agora você pode adicionar a data de entrada
    df['Data_Entrou_Carteira'] = np.where(
        df['Raiz_CNPJ'].isin(referencia['Raiz_CNPJ']),
        pd.Timestamp('2025-03-20'),
        pd.NaT
    )

    # Lógica de status
    data_limite = datetime.today() - timedelta(days=6*30)

    df['Faturamento_6_Meses'] = pd.to_numeric(df['Faturamento_6_Meses'], errors='coerce').fillna(0)

    df['Status_Cliente'] = df['Data_Ultima_Venda_Grupo_CNPJ'].apply(
        lambda x: 'Nao Compra' if pd.isna(x) or x < data_limite else 'Compra'
    )

    # --- Garantir tipos corretos ---
    df['Data_Ultimo_Contato'] = pd.to_datetime(df['Data_Ultimo_Contato'], errors='coerce')
    df['Data_Ultimo_Followup'] = pd.to_datetime(df['Data_Ultimo_Followup'], errors='coerce')
    df['Data_Entrou_Carteira'] = pd.to_datetime(df['Data_Entrou_Carteira'], errors='coerce')
    df['Data_Ultimo_Orcamento'] = pd.to_datetime(df['Data_Ultimo_Orcamento'], errors='coerce')

    # --- Contatos e Follow-ups após rotação ---
    df['Total_Contatos_Rotacao'] = df.apply(
        lambda row: row['Total_Contatos'] if pd.notna(row['Data_Entrou_Carteira']) and
                                            pd.notna(row['Data_Ultimo_Contato']) and
                                            pd.notna(row['data_ultima_rotacao']) and
                                            row['Data_Ultimo_Contato'] >= row['Data_Entrou_Carteira']
                    else 0,
        axis=1
    )

    df['Total_Followups_Rotacao'] = df.apply(
        lambda row: row['Total_Followups'] if pd.notna(row['Data_Entrou_Carteira']) and
                                            pd.notna(row['Data_Ultimo_Followup']) and
                                            pd.notna(row['data_ultima_rotacao']) and
                                            row['Data_Ultimo_Followup'] >= row['Data_Entrou_Carteira']
                    else 0,
        axis=1
    )

    df['Total_Orcamentos_Rotacao'] = df.apply(
    lambda row: row['Total_Orcamentos'] if pd.notna(row['Data_Entrou_Carteira']) and
                                              pd.notna(row['Data_Ultimo_Orcamento']) and
                                              pd.notna(row['data_ultima_rotacao']) and
                                              row['Data_Ultimo_Orcamento'] >= row['Data_Entrou_Carteira']
                else 0,
    axis=1
    )

    df['Total_Oportunidades_Rotacao'] = df.apply(
        lambda row: row['Total_Oportunidades'] if pd.notna(row['Data_Entrou_Carteira']) and
                                                pd.notna(row['Data_Ultima_Oportunidade']) and
                                                pd.notna(row['data_ultima_rotacao']) and
                                                row['Data_Ultima_Oportunidade'] >= row['Data_Entrou_Carteira']
                    else 0,
        axis=1
    )


    df_historico = df[['Raiz_CNPJ', 'Nome_Vendedor']].dropna().drop_duplicates().reset_index(drop=True)

    df_filtrado = df[df['Nome_Vendedor'].isin(vendedores_ativos)].reset_index(drop=True)

    contas_vao_rotacionar = df[
        (df['Status_Cliente'] == 'Nao Compra') &
        (df['Data_Abertura_Conta'] < data_limite) &
        ((df['Data_Entrou_Carteira'] < data_limite) | (df['Data_Entrou_Carteira'].isnull())) &
        ((df['Grupo_Econômico_ID'].isnull()) | (df['Grupo_Econômico_ID'] == '')) &
        (df['Faturamento_6_Meses'] == 0)
    ]

    # Lógica de filtragem baseada na opção selecionada
    if "Helder" in opcao:
        contas_filtradas = contas_vao_rotacionar[contas_vao_rotacionar['Classificacao_Conta'].isin([5, 7])]
    elif "Karen" in opcao:
        contas_filtradas = contas_vao_rotacionar[~contas_vao_rotacionar['Classificacao_Conta'].isin([5, 7])]
    else: # Novo grupo
        contas_filtradas = contas_vao_rotacionar
        
    def registrar_historico_rotacao(nome_vendedor, conta_id, tipo_rotacao, data_rotacao):
        conn = sqlite3.connect('historico_rotacao.db')
        c = conn.cursor()
        c.execute('''
        INSERT INTO historico_rotacao (nome_vendedor, conta_id, tipo_rotacao, data_rotacao)
        VALUES (?, ?, ?, ?)
        ''', (nome_vendedor, conta_id, tipo_rotacao, data_rotacao))
        conn.commit()
        conn.close()

    # ---------- FUNÇÃO DE ROTAÇÃO ----------
    def rotacionar_contas(df_contas, lista_vendedores, df_historico, limite_por_vendedor=50):
        contagem_vendedores = {v: 0 for v in lista_vendedores}
        novos_nomes = []
        indices_sobras = []

        for idx, row in df_contas.iterrows():
            cnpj = row['Raiz_CNPJ']
            vendedores_antigos = df_historico[df_historico['Raiz_CNPJ'] == cnpj]['Nome_Vendedor'].tolist()
            candidatos = [v for v in lista_vendedores if v not in vendedores_antigos and contagem_vendedores[v] < limite_por_vendedor]
            if candidatos:
                escolhido = np.random.choice(candidatos)
                contagem_vendedores[escolhido] += 1
                novos_nomes.append((idx, escolhido))
            else:
                indices_sobras.append(idx)

        df_resultado = df_contas.copy()
        data_hoje = pd.Timestamp.today().normalize()

        for idx, novo_vendedor in novos_nomes:
            df_resultado.at[idx, 'Nome_Vendedor'] = novo_vendedor
            df_resultado.at[idx, 'Data_Entrou_Carteira'] = data_hoje

            # Registrar histórico no banco
        for idx, novo_vendedor in novos_nomes:
            conta_id = df_contas.at[idx, 'Conta_ID']
            registrar_historico_rotacao(
                nome_vendedor=novo_vendedor,
                conta_id=conta_id,
                tipo_rotacao='Automática',
                data_rotacao=data_hoje.strftime('%Y-%m-%d')
            )

        df_rotacionadas = df_resultado.loc[[idx for idx, _ in novos_nomes]].reset_index(drop=True)
        df_sobras = df_resultado.loc[indices_sobras].reset_index(drop=True)

        return df_rotacionadas, df_sobras
    
    def registrar_historico_rotacao(nome_vendedor, conta_id, tipo_rotacao, data_rotacao):
        conn = sqlite3.connect('historico_rotacao.db')
        c = conn.cursor()
        c.execute('''
            INSERT INTO historico_rotacao (nome_vendedor, conta_id, tipo_rotacao, data_rotacao)
            VALUES (?, ?, ?, ?)
        ''', (nome_vendedor, conta_id, tipo_rotacao, data_rotacao))
        conn.commit()
        conn.close()


    # Botão de rotação
st.markdown('#### 2-Clique no botão para rotacionar.')
if st.button("🔁 Rodar contas agora"):
    contas_rotacionadas, contas_sobras = rotacionar_contas(contas_filtradas, vendedores_ativos, df_historico)

    st.success(f"Foram encontradas {len(contas_filtradas)} clientes disponiveis para rotação e {len(contas_rotacionadas)} foram rotacionados com sucesso.")
    st.write("Contas rotacionadas:")
    st.dataframe(contas_rotacionadas)
    st.session_state["contas_rotacionadas"] = contas_rotacionadas
    st.session_state["contas_sobras"] = contas_sobras

    # Histórico
    historico_path = "historico_rotacoes_completo.xlsx"
    if os.path.exists(historico_path):
        historico_existente = pd.read_excel(historico_path)
        df_novos_historicos = pd.concat([historico_existente, contas_rotacionadas], ignore_index=True)
    else:
        df_novos_historicos = contas_rotacionadas.copy()

    df_novos_historicos = df_novos_historicos.drop_duplicates(subset=["Raiz_CNPJ", "Data_Entrou_Carteira"], keep="last")
    df_novos_historicos.to_excel(historico_path, index=False)

    st.write("Contas sem rotação (sem vendedor disponível):")
    st.dataframe(contas_sobras)

def gerar_excel_download(df):
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Planilha1')
    return output.getvalue()

if "contas_rotacionadas" in st.session_state:
    st.markdown('#### 3-Faça o Download das contas rotacionadas e armazene no servidor')
    st.markdown('👇 Clique no botão abaixo para fazer o download do historico de rotação.')
    st.download_button(
        "📥 Baixar contas rotacionadas",
        data=gerar_excel_download(st.session_state["contas_rotacionadas"]),
        file_name=f"historico_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    )

st.markdown("---")

st.subheader("📊 Gerar Relatórios por Vendedor")
st.markdown('👇 Clique no botão abaixo para fazer o download dos relatórios de rotação.')

if st.button("📄 Gerar Relatório Completo e por Vendedor"):
    if "contas_rotacionadas" in st.session_state:
        df_atual = st.session_state["contas_rotacionadas"].copy()
        st.success("✅ Usando contas rotacionadas para o relatório.")
    else:
        df_atual = df_filtrado.copy()
        st.warning("⚠️ Nenhuma rotação foi realizada. Usando base atual para gerar relatório.")

    def gerar_relatorios(df_atual, df_anterior, data_limite, data_rotacao, pasta_destino='Relatorio_Rotação'):
        os.makedirs(pasta_destino, exist_ok=True)

        data_rotacao = pd.to_datetime(data_rotacao).normalize()
        data_limite = pd.to_datetime(data_limite).normalize()

        for df in [df_atual, df_anterior]:
            df['Data_Entrou_Carteira'] = pd.to_datetime(df['Data_Entrou_Carteira'], errors='coerce')
            df['Data_Ultima_Venda_Grupo_CNPJ'] = pd.to_datetime(df['Data_Ultima_Venda_Grupo_CNPJ'], errors='coerce')

        vendedores = df_atual['Nome_Vendedor'].dropna().unique()
        arquivos_por_vendedor = {}

        writer = pd.ExcelWriter(f'{pasta_destino}/relatorio_mensal_completo.xlsx', engine='xlsxwriter')

        for vendedor in vendedores:
            atual_vend = df_atual[df_atual['Nome_Vendedor'] == vendedor].copy()
            anterior_vend = df_anterior[df_anterior['Nome_Vendedor'] == vendedor].copy()

            def montar_bloco(df, status):
                bloco = df[[
                    'Nome_Vendedor',
                    'Razao_Social_Pessoas',
                    'Raiz_CNPJ',
                    'Faturamento_6_Meses',
                    'Total_Pedidos',
                    'Data_Ultima_Venda_Grupo_CNPJ',
                    'Data_Entrou_Carteira',
                    'data_ultima_rotacao',
                    'Total_Contatos_Rotacao',
                    'Data_Ultimo_Contato',
                    'Total_Followups_Rotacao',
                    'Data_Ultimo_Followup',
                    'Total_Orcamentos_Rotacao',
                    'Data_Ultimo_Orcamento'
                ]].copy()
                bloco.insert(0, 'Status', status)
                return bloco

            usados = set()
            blocos = []

            ativas = anterior_vend[
                (
                    (anterior_vend['Data_Ultima_Venda_Grupo_CNPJ'] >= data_limite) |
                    (anterior_vend['Grupo_Econômico_ID'].notnull())
                ) &
                (~anterior_vend['Raiz_CNPJ'].isin(usados))
            ]
            usados.update(ativas['Raiz_CNPJ'])
            blocos.append(montar_bloco(ativas, 'Ativa'))

            seis_meses_atras = data_rotacao - pd.DateOffset(months=6)
            recentes = anterior_vend[
                (anterior_vend['Data_Entrou_Carteira'] >= seis_meses_atras) &
                (anterior_vend['Data_Entrou_Carteira'] != data_rotacao) &
                (~anterior_vend['Raiz_CNPJ'].isin(usados))
            ]
            usados.update(recentes['Raiz_CNPJ'])
            blocos.append(montar_bloco(recentes, 'Entraram Recentemente'))

            novas = atual_vend[
                (atual_vend['Data_Entrou_Carteira'] == data_rotacao) &
                (~atual_vend['Raiz_CNPJ'].isin(usados))
            ]
            usados.update(novas['Raiz_CNPJ'])
            blocos.append(montar_bloco(novas, 'Novas Recebidas'))

            cadastradas_recente = anterior_vend[
                (anterior_vend['Data_Abertura_Conta'] >= seis_meses_atras) &
                (~anterior_vend['Raiz_CNPJ'].isin(usados))
            ]
            usados.update(cadastradas_recente['Raiz_CNPJ'])
            blocos.append(montar_bloco(cadastradas_recente, 'Cadastrado Recentemente'))

            retiradas = anterior_vend[
                (~anterior_vend['Raiz_CNPJ'].isin(atual_vend['Raiz_CNPJ'])) &
                (~anterior_vend['Raiz_CNPJ'].isin(usados))
            ]
            usados.update(retiradas['Raiz_CNPJ'])
            blocos.append(montar_bloco(retiradas, 'Retiradas'))

            df_relatorio = pd.concat(blocos, ignore_index=True)
            df_relatorio = df_relatorio.drop_duplicates(subset='Raiz_CNPJ', keep='first')
            df_relatorio = df_relatorio.sort_values(['Status', 'Razao_Social_Pessoas']).reset_index(drop=True)

            if not df_relatorio.empty:
                nome_arquivo_vendedor = f"{pasta_destino}/relatorio_{vendedor.replace(' ', '_')}_{data_rotacao.strftime('%Y-%m-%d')}.xlsx"
                df_relatorio.to_excel(nome_arquivo_vendedor, index=False)
                arquivos_por_vendedor[vendedor] = nome_arquivo_vendedor

                aba = vendedor[:31]
                df_relatorio.to_excel(writer, sheet_name=aba, index=False)

        writer.close()
        return arquivos_por_vendedor


    arquivos_gerados = gerar_relatorios(
        df_atual=df_atual,
        df_anterior=df_filtrado,
        data_limite=data_limite,
        data_rotacao=pd.Timestamp.today().normalize(),
        pasta_destino='Relatorio_Rotação'
    )

    st.success("✅ Relatórios gerados com sucesso!")

    zip_file_path = os.path.join(tempfile.gettempdir(), "relatorios_rotacao.zip")
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        zipf.write('Relatorio_Rotação/relatorio_mensal_completo.xlsx', 'relatorio_mensal_completo.xlsx')
        for vendedor, arquivo in arquivos_gerados.items():
            zipf.write(arquivo, arquivo.split('/')[-1])

    with open(zip_file_path, 'rb') as f:
        st.download_button(
            label="📥 Baixar Todos os Relatórios",
            data=f,
            file_name="relatorios_rotacao.zip",
            mime="application/zip"
        )