Padrão de Notebook Big Table — Databricks PREVI

Fonte: Análise de 200+ tabelas nos schemas efinanceira, suporte_a_operacao, operacoes_com_participantes e previ_futuro. Referência modelo: silver_pfam_efinanc_conferencia_mod_previdenciario.

O que é uma Big Table

Big Table é uma tabela Silver que consolida múltiplas fontes (Bronze, Silver ou Gold) via JOINs, produzindo uma visão desnormalizada para conferência, análise ou consumo.

Diferente da Silver comum (1 fonte → 1 tabela), a Big Table cruza 2–5 tabelas e pode gerar colunas derivadas (flags, diferenças, cálculos).

Aspecto	Silver comum	Big TableFontes	1 tabela/view de origem	2–5 tabelas Bronze/Silver/Gold
Linguagem do ETL	SQL (temp view)	PySpark (DataFrames)
JOINs	Nenhum ou 1 simples	Múltiplos LEFT JOINs
Colunas derivadas	Raras	Flags de divergência, cálculos, agregações
Notebook	Standalone	Standalone
Prefixo da tabela	silver_	silver_ (mesma camada)
Convenções Obrigatórias
Artefato	PadrãoNotebook	nb_big_table_
Tabela destino	..silver_
Workspace	/Workspace///BIG_TABLE/
DataFrame final	df_ (ex: df_conferencia)
Regra Fundamental: NUNCA usar sandbox como fonte

Big Tables sempre leem de tabelas Bronze, Silver ou Gold oficiais do catálogo.

Tabelas de sandbox (sandbox.db_sandbox_<usuario>.*) são objetos pessoais e instáveis. O proprietário pode alterar, recriar ou excluir a tabela sem aviso.

Se a demanda veio de uma query de sandbox, reproduzir a lógica a partir das fontes oficiais e não fazer FROM diretamente no sandbox.

Metadados Obrigatórios
Campo	ValorStory	
Épico	
Tabela Silver (destino)	..silver_
Fontes	Lista das tabelas fully qualified
Referência de padrão	Tabela semelhante já existente
Responsável	<NOME_SOBRENOME>
Squad	
Owner de Dados	<AREA_SIGLA>
Estrutura Obrigatória
Plain Text
1
nb_big_table_<assunto>
2
 
3
├── Célula 1 — Documentação (Markdown)
4
├── Célula 2 — Exploração das fontes (SQL)
5
├── Célula 3 — ETL Big Table (Python/PySpark)
6
├── Células 4.x — Validações (1 ou mais células)
7
├── Escrita Silver (Python)
8
└── Documentação do catálogo (SQL)
Mostrar mais linhas
Célula 1 — Documentação (Markdown)

Contém obrigatoriamente:

Metadados da story
Seção "Fontes e JOINs"
Alias, tabela, chave e estratégia dos JOINs
Matriz de mapeamento de colunas
Campo origem
Tabela/alias de origem
Tipo de origem
Tipo final
Status
Changelog técnico (ordem decrescente)

Exemplo:

Plain Text
1
CPF | CPF | STRING | direto | OK
2
dif_cpf | BOOLEAN | derivado | ⚠️ confirmar DO
Mostrar mais linhas

Colunas com lógica de comparação ou regra de negócio ainda não validada devem receber:

Plain Text
1
⚠️ confirmar DO
2
 
Mostrar mais linhas
Célula 2 — Exploração das Fontes (SQL)

Listar todas as fontes oficiais.

Apenas a primeira fonte deve ficar descomentada.

As demais devem permanecer comentadas para exploração sob demanda.

Exemplo:

SQL
1
-- Fonte 3: Cadastro PFam
2
 
3
-- SELECT *
4
-- FROM `catalogo-previ-prod`.suporte_a_operacao.gold_cpvexpcaptfam
5
-- LIMIT 20
Mostrar mais linhas
Célula 3 — ETL Big Table (Python/PySpark)

Célula principal de transformação.

Estrutura recomendada:

Python
1
# ── 1. Parâmetros ───────────────────────────────
2
 
3
# ── 2. Leitura das fontes ───────────────────────
4
 
5
# ── 3. JOINs e regras de negócio ────────────────
6
 
7
# ── 4. Seleção final ────────────────────────────
8
 
9
# ── 5. Inspeção visual ──────────────────────────
10
display(df_final)
Mostrar mais linhas
Boas práticas
Utilizar aliases curtos e consistentes.
Utilizar LEFT JOIN a partir da tabela base.
Utilizar INNER JOIN apenas quando obrigatório.
Aplicar F.coalesce() antes de operações aritméticas.
Fazer cast explícito quando houver mudança de tipo.
Executar display(df_final) antes das validações.
Não utilizar cache() ou persist() sem justificativa.
Células 4.x — Validações

As validações podem ser divididas em quantas células forem necessárias.

O objetivo é separar responsabilidades e facilitar análise, troubleshooting e aprovação da escrita.

Regra Principal

A escrita da Silver somente poderá ser executada após todas as validações previstas para a story terem sido executadas, analisadas e aprovadas.

A ausência de erro de execução não significa que a ETL está correta.

Uma Big Table pode executar sem erro e ainda produzir dados incorretos devido a:

JOINs sem correspondência
Chaves erradas
Multiplicação de registros
Divergências artificiais
Erros de regra de negócio
Validações Obrigatórias
4.1 Validação de Volume

Objetivo:

Garantir que o volume produzido seja compatível com a expectativa da regra de negócio.

Exemplos:

Python
1
df_base.count()
2
df_final.count()
Mostrar mais linhas
4.2 Validação de Nulos

Objetivo:

Garantir preenchimento adequado das colunas críticas.

Exemplos:

Python
1
CPF
2
INSCRICAO
3
VALOR
4
DATA_REF
Mostrar mais linhas
4.3 Validação de Duplicidades

Objetivo:

Identificar possíveis multiplicações de registros ou duplicidade de chave.

Exemplo:

Python
1
df_final.groupBy(
2
"INSCRICAO",
3
"ANO",
4
"MES"
5
).count()
6
``
Mostrar mais linhas

Caso existam duplicidades legítimas, documentar a justificativa.

4.4 Validação de Efetividade dos JOINs

Obrigatória para toda Big Table.

Objetivo:

Garantir que os JOINs estão produzindo correspondências reais.

Exemplos:

Python
1
df_final.filter(F.col("CPF").isNotNull()).count()
2
 
3
df_final.filter(F.col("NUM_CNPJ").isNotNull()).count()
4
 
5
df_final.filter(F.col("VAL_PORTA").isNotNull()).count()
Mostrar mais linhas

Essa validação existe para evitar casos em que:

o notebook executa;
a tabela é criada;
mas todos os campos oriundos dos JOINs ficam nulos.

Também pode incluir:

percentual de match;
análise de chaves não encontradas;
comparação entre chaves das tabelas participantes.
4.5 Validação Funcional

Obrigatória quando a Big Table possui regras de conferência.

Exemplo:

Python
1
df_final.select(
2
F.count(
3
F.when(
4
F.col("dif_cpf") == True,
5
1
6
)
7
).alias("divergencias_cpf"),
8
 
9
F.count(
10
F.when(
11
F.col("dif_valor") != 0,
12
1
13
)
14
).alias("divergencias_valor")
15
).show()
Mostrar mais linhas
4.6 Validações Específicas da Story

Opcional.

Exemplos:

Reconciliação financeira
Consistência temporal
Conferência regulatória
Regras definidas pelo Data Owner
Critério de Aprovação da Escrita

A Escrita Silver NÃO deve ser executada quando houver:

JOINs sem correspondência não explicados;
Volume incompatível;
Multiplicação indevida de registros;
Níveis críticos de nulos;
Divergências sem validação do Data Owner;
Evidências de erro de regra de negócio.
Escrita Silver (Python)

Executar somente após aprovação das validações.

Padrão:

Python
1
df_final.write \
2
.format("delta") \
3
.option("overwriteSchema", "true") \
4
.mode("overwrite") \
5
.saveAsTable(SILVER_TABLE)
6
 
7
print(f"✓ Gravado em {SILVER_TABLE}: {df_final.count():,} linhas")
Mostrar mais linhas

Escrita padrão:

Plain Text
1
overwrite
2
+
3
overwriteSchema=true
Mostrar mais linhas

Casos de carga incremental ou histórica devem ser analisados individualmente.

Documentação do Catálogo (SQL)
SQL
1
COMMENT ON TABLE `<catalogo>`.`<schema>`.silver_<assunto>
2
IS '<Descrição da Big Table. Informar todas as fontes oficiais utilizadas.>';
3
 
4
ALTER TABLE `<catalogo>`.`<schema>`.silver_<assunto>
5
ALTER COLUMN COLUNA_1 COMMENT
6
'Descrição de negócio. Fonte: tabela.coluna';
Mostrar mais linhas
Regras
Toda coluna deve possuir rastreabilidade.
Informar a fonte original da informação.
Explicar flags de divergência.
Explicar fórmulas monetárias.
Utilizar linguagem de negócio.
O comentário da tabela deve listar todas as fontes oficiais utilizadas.
Padrão de JOINs
Ordem	Tipo	Quando usarTabela base	—	Maior granularidade
Dados complementares	LEFT JOIN	Enriquecimento
Dados obrigatórios	INNER JOIN	Exceção
Chaves comuns
Plain Text
1
INSCRICAO + ANO + MES
2
CPF + DATA_REF
3
NUMERO_PROPOSTA + COMPETENCIA
Mostrar mais linhas

Antes do JOIN, avaliar:

nulos na chave;
granularidade;
unicidade;
compatibilidade temporal;
transformações necessárias na chave.
Tipos de Colunas Derivadas
Tipo	PadrãoFlag booleana	(F.col("a.X") != F.col("b.Y"))
Diferença monetária	a.VAL - coalesce(b.VAL, 0)
Semestre derivado	when(mes <= 6,1).otherwise(2)
Campo auxiliar	F.lit(None)
Concatenação	ANO/MES
Checklist de Entrega
Plain Text
1
□ Notebook nomeado corretamente
2
□ Fontes oficiais utilizadas
3
□ Nenhuma dependência de sandbox
4
□ JOINs documentados
5
□ Colunas derivadas documentadas
6
□ Validação de volume executada
7
□ Validação de nulos executada
8
□ Validação de duplicidades executada
9
□ Validação de efetividade dos JOINs executada
10
□ Validação funcional executada (quando aplicável)
11
□ Todas as validações aprovadas
12
□ Escrita Silver executada
13
□ COMMENT ON TABLE criado
14
□ ALTER COLUMN COMMENT criado
15
□ Working Notes atualizado
Mostrar mais linhas
Template Working Notes
Plain Text
1
- Data: <AAAA-MM-DD>
2
- Atualização: Big Table <assunto> criada/atualizada.
3
- Fontes: <lista das tabelas oficiais>.
4
- Bloqueio: <nenhum ou descrição>.
5
- Próxima ação: <passo imediato>.
Mostrar mais linhas
Referência de Exemplo
Plain Text
1
nb_big_table_modulo_portabilidade
2
 
3
Story: STRY0028272
4
Schema: efinanceira
5
 
6
Fontes:
7
- silver_cpv_exp_port_entr
8
- silver_pfam_efinanc_rel_mod_portabilidade
9
- gold_cpvexpcaptfam
10
 
Mostrar mais linhas
Regras de Uso com IA
Nunca utilizar sandbox como fonte.
Reproduzir a lógica usando fontes oficiais.
Toda coluna derivada deve estar documentada.
Colunas com regra incerta devem receber ⚠️ confirmar DO.
Não utilizar DELETE, DROP TABLE ou TRUNCATE.
Não usar cache() ou persist() sem justificativa.
Não remover filtros de negócio sem registrar no changelog.
Priorizar PySpark legível, auditável e organizado por blocos.
Nunca expor dados sensíveis em prompts, comentários ou d
