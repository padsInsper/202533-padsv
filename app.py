import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

from shiny import App, ui, render, reactive

BASE = Path('./data/')

dt = pd.read_csv(BASE / "dim_teachers.csv", dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])
fe = pd.read_csv(BASE / "fct_teachers_entries.csv", dtype=str)
fci = pd.read_csv(BASE / "fct_teachers_contents_interactions.csv", dtype=str)
sf = pd.read_csv(BASE / "stg_formation.csv", dtype=str)
mc = pd.read_csv(BASE / "stg_mari_ia_conversation.csv", dtype=str)
mr = pd.read_csv(BASE / "stg_mari_ia_reports.csv", dtype=str)

for c in ["data_entrada"]:
    if c in dt.columns:
        dt[c] = pd.to_datetime(dt[c], errors="coerce")

for df, cols in [(fe, ["data_inicio","data_fim"]),
                 (fci, ["data_inicio"]),
                 (sf, ["createdat","updatedat"]),
                 (mc, ["createdat","updatedat"]),
                 (mr, ["updatedat"])]:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

def norm_id(s):
    return s.astype(str).str.strip().str.lower()

dt["unique_id"] = norm_id(dt["unique_id"])

for df in (fe, fci):
    if "unique_id" in df.columns:
        df["unique_id"] = norm_id(df["unique_id"])

if "userid" in sf.columns:
    sf["unique_id"] = norm_id(sf["userid"])

for df in (mc, mr):
    if "unique_id_aprendizap" in df.columns:
        df["unique_id"] = norm_id(df["unique_id_aprendizap"])

def fk_coverage(dim_ids: pd.Series, fact_ids: pd.Series, fact_name: str):
    """Relata cobertura de FK do fato em relação à dimensão central."""
    dim_ids = set(dim_ids.dropna())
    fact_ids = set(fact_ids.dropna())
    inter = dim_ids & fact_ids
    only_fact = fact_ids - dim_ids
    return {
        "fato": fact_name,
        "ids_no_fato": len(fact_ids),
        "ids_na_dim": len(dim_ids),
        "ids_em_ambos": len(inter),
        "ids_somente_no_fato": len(only_fact),
        "pct_fato_com_dim": (len(inter) / max(1, len(fact_ids))) * 100,
    }

def null_share(df: pd.DataFrame, cols: list[str]):
    """% de nulos por coluna (útil para antes/depois de filtros)."""
    out = []
    n = len(df)
    for c in cols:
        if c in df.columns:
            out.append({"coluna": c, "nulos": df[c].isna().sum(), "pct_nulos": 100*df[c].isna().sum()/max(1,n)})
    return pd.DataFrame(out).sort_values("pct_nulos", ascending=False)

dim_ids = dt["unique_id"]

audits = []
if "unique_id" in fe.columns: audits.append(fk_coverage(dim_ids, fe["unique_id"], "fct_teachers_entries"))
if "unique_id" in fci.columns: audits.append(fk_coverage(dim_ids, fci["unique_id"], "fct_teachers_contents_interactions"))
if "unique_id" in sf.columns: audits.append(fk_coverage(dim_ids, sf["unique_id"], "stg_formation"))
if "unique_id" in mc.columns: audits.append(fk_coverage(dim_ids, mc["unique_id"], "stg_mari_ia_conversation"))
if "unique_id" in mr.columns: audits.append(fk_coverage(dim_ids, mr["unique_id"], "stg_mari_ia_reports"))

audit_fk = pd.DataFrame(audits).sort_values("pct_fato_com_dim", ascending=False)

cols_check = [
    "profid","utm_origin","tela_origem","estado","total_alunos","login_google",
    "currentstage","currentsubject","selectedstages","selectedsubjectsem",
    "selectedsubjectsfundii","visualizou_metodologia_ativa"
]

dt_f = dt[dt["currentstage"].notna()].copy()

def join_coverage(dim_df: pd.DataFrame, fact_df: pd.DataFrame, fact_name: str):
    if "unique_id" not in fact_df.columns: 
        return None
    dim_ids = set(dim_df["unique_id"].dropna())
    fact_ids = set(fact_df["unique_id"].dropna())
    inter = dim_ids & fact_ids
    return {
        "fato": fact_name,
        "ids_dim": len(dim_ids),
        "ids_fato": len(fact_ids),
        "ids_joinaveis": len(inter),
        "pct_fato_alcancado": 100*len(inter)/max(1,len(fact_ids))
    }

coverage_before = []
coverage_after  = []
for fact, name in [(fe,"entries"), (fci,"contents_interactions"), (sf,"formation"), (mc,"mari_conversation"), (mr,"mari_reports")]:
    if "unique_id" in fact.columns:
        coverage_before.append(join_coverage(dt, fact, name))
        coverage_after.append(join_coverage(dt_f, fact, name))


# Exemplo: marcar como ativo quem teve QUALQUER evento (entries, contents, mari, formação) na última semana do dado disponível.
# Define a janela pelo max de datas observadas em cada fonte:
def max_date(*series):
    s = pd.concat(series)
    return s.max()

last_date = max_date(
    fe.get("data_inicio", pd.Series([], dtype="datetime64[ns]")),
    fci.get("data_inicio", pd.Series([], dtype="datetime64[ns]")),
    sf.get("updatedat", pd.Series([], dtype="datetime64[ns]")),
    mc.get("updatedat", pd.Series([], dtype="datetime64[ns]")),
    mr.get("updatedat", pd.Series([], dtype="datetime64[ns]")),
)

# Janela WAU = [last_date - 6 dias, last_date]
wau_start = None
if pd.notna(last_date):
    wau_start = last_date.normalize() - pd.Timedelta(days=6)

def active_ids_in_window(df, id_col, date_col, start, end):
    if id_col not in df.columns or date_col not in df.columns or start is None:
        return set()
    m = df[date_col].between(start, end, inclusive="both")
    return set(df.loc[m, id_col].dropna().map(str).str.lower())

active_entries = active_ids_in_window(fe, "unique_id", "data_inicio", wau_start, last_date)
active_contents = active_ids_in_window(fci, "unique_id", "data_inicio", wau_start, last_date)
active_formation = active_ids_in_window(sf, "unique_id", "updatedat", wau_start, last_date)
active_mari = active_ids_in_window(mc, "unique_id", "updatedat", wau_start, last_date) | \
              active_ids_in_window(mr, "unique_id", "updatedat", wau_start, last_date)

active_any = active_entries | active_contents | active_formation | active_mari

dt["is_active_wau"] = dt["unique_id"].isin(active_any)



# A) Base consolidada mínima (todos na DIM + flags de atividade)
cols_keep = ["unique_id","estado","currentstage","currentsubject","total_alunos","data_entrada","is_active_wau"]
base_min = dt[cols_keep].copy()

# B) Base orientada a engajamento (exige pelo menos 1 atividade em qualquer fato)
base_engajada = base_min[base_min["is_active_wau"]]

# C) Base orientada a perfil (exige currentstage não nulo, mas sem exigir atividade)
base_perfil = base_min[base_min["currentstage"].notna()]


#interactions
fci["is_active_wau"] = fci["unique_id"].isin(active_any)
fci_engajada = fci[fci["is_active_wau"]]

#entries
fe["is_active_wau"] = fe["unique_id"].isin(active_any)
fe_engajada = fe[fe["is_active_wau"]]

#reset indexes
base_engajada.reset_index(drop=True, inplace=True)
fci_engajada.reset_index(drop=True, inplace=True)
fe_engajada.reset_index(drop=True, inplace=True)

# Replace 'ensino_medio' with 'em' using .loc
base_engajada.loc[base_engajada['currentstage'] == 'ensino_medio', 'currentstage'] = 'em'

# Filter out NaN and 'all' values
base_engajada = base_engajada[
    base_engajada['currentstage'].notna() &
    (base_engajada['currentstage'] != 'all')
].copy()



# Merge fci_engajada with base_engajada on 'unique_id'
base_eventos = fci_engajada.merge(
    base_engajada[['unique_id', 'estado', 'currentstage', 'currentsubject', 'total_alunos', 'data_entrada']],
    on='unique_id',
    how='left'
)

# Select and reorder the desired columns
base_eventos = base_eventos[[
    'unique_id', 'data_inicio', 'event_type',
    'estado', 'currentstage', 'currentsubject',
    'total_alunos', 'data_entrada'
]]

# Drop rows where 'currentstage' is NaN
base_eventos = base_eventos.dropna(subset=['currentstage']).copy(deep=True)

# Ensure datetime format
base_eventos['data_inicio'] = pd.to_datetime(base_eventos['data_inicio'], errors='coerce')

# Create 6-month period labels
base_eventos['period'] = base_eventos['data_inicio'].dt.to_period('6M')

# Subject label mapping
subject_map = {
    '1': "Português", '2': "História", '3': "Geografia", '4': "Arte", '5': "Matemática",
    '6': "Ciências", '7': "Inglês", '8': "Linguagens",
    'linguagens': "Linguagens", 'humanas': "Ciênc. Humanas", 'ciencias': "Ciências",
    'vida': "Projeto de Vida", 'matematica': "Matemática"
}

# Map subject labels
base_eventos['subject_label'] = base_eventos['currentsubject'].map(subject_map)


# Monthly events per teacher per subject
base_eventos['period'] = base_eventos['data_inicio'].dt.to_period('M')

# Filter for Linguagens and Inglês
filtered_df = base_eventos.copy()

# Convert date and extract year-month
filtered_df['data_inicio'] = pd.to_datetime(filtered_df['data_inicio'], errors='coerce')
filtered_df['year_month'] = filtered_df['data_inicio'].dt.to_period('M').dt.to_timestamp()

# Filter to include only weeks up to June 2025
filtered_df = filtered_df[filtered_df['data_inicio'] <= pd.Timestamp('2025-07-30')]

# Get all unique subjects
all_subjects = sorted(filtered_df['subject_label'].dropna().unique())


from shiny import ui

app_ui = ui.page_fluid(
    # Global styles
    ui.tags.style("""
        body {
            font-family: 'Segoe UI', sans-serif;
            background-color: #f4f6f9;
            color: #333;
        }
        h2, h3 {
            font-weight: 600;
            color: #2c3e50;
        }
        .custom-sidebar {
            background-color: #dbe4f0;
            padding: 20px;
            border-radius: 12px;
            color: #2c3e50;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .section {
            background-color: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        }
    """),

    # Sidebar + Main layout
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_checkbox_group("subjects", "Selecione as matérias:",
                choices=all_subjects,
                selected=all_subjects
            ),
            ui.input_date_range("date_range", "Selecione o intervalo de datas:",
                start=filtered_df['data_inicio'].min().date(),
                end=filtered_df['data_inicio'].max().date()
            ),
            class_="custom-sidebar"
        ),

        # Main content
        ui.div(
            ui.div(
                ui.h2("Análise de Participação por Matéria"),
                ui.p("Este gráfico interativo mostra o número de usuários únicos por mês, separados por matéria. Use os filtros à esquerda para personalizar sua visualização."),
                ui.output_plot("user_plot", height="400px"),
                class_="section"
            )
        )
    )
)



# Server logic
def server(input, output, session):
    # @reactive.calc
    # def filtered_data():
    #     start_date = pd.to_datetime(input.date_range()[0])
    #     end_date = pd.to_datetime(input.date_range()[1])
    #     return filtered_df[
    #         (filtered_df['subject_label'].isin(input.subjects())) &
    #         (filtered_df['data_inicio'] >= start_date) &
    #         (filtered_df['data_inicio'] <= end_date)
    #     ]
    qc = querychat.server("chat", qc_config)

    @render.data_frame
    def filtered_data():
        return qc.df()

    @reactive.calc
    def user_counts():
        return (
            filtered_data()
            .groupby(['subject_label', 'year_month'])['unique_id']
            .nunique()
            .reset_index()
            .rename(columns={'unique_id': 'user_count'})
        )
    
    
    @output
    @render.plot
    def user_plot():
        fig = plt.figure(figsize=(10, 4))

        # Sort the data by year_month to ensure proper x-axis order
        df_plot = user_counts().sort_values('year_month')

        sns.lineplot(data=df_plot, x='year_month', y='user_count',
                    hue='subject_label', palette='viridis')

        plt.xticks(rotation=45)
        plt.title('Número de usuários núicos por mês')
        plt.xlabel('Mês')
        plt.ylabel('Usuários únicos')
        plt.tight_layout()
        return fig


# Launch the app
app = App(app_ui, server)
