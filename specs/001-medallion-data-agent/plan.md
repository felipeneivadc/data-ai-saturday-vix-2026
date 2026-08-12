# Plano de Implementação

## Visão Técnica
A solução utiliza arquitetura medalhão com processamento em notebooks do Fabric para transformar dados brutos em um modelo analítico pronto para uso em relatórios e agentes conversacionais.

## Componentes da Solução

### 1. Bronze
- Armazenamento de arquivos de origem em formato bruto.
- Preservação de rastreabilidade por nome do arquivo e metadados do processo.
- Leitura inicial via CSV com schema explícito.

### 2. Silver
- Limpeza e padronização dos pedidos.
- Normalização de nomes, emails e datas.
- Exclusão de duplicatas com base em chave de negócio.
- Criação de colunas de auditoria e flag de qualidade.

### 3. Gold
- Criação da dimensão Date, Customer e Product.
- Geração da tabela fato Sales com métricas calculadas.
- Uso de chaves substitutas para manter consistência do modelo dimensional.
- Registro da tabela no catálogo para integração com o modelo semântico.

## Fluxo de Dados
1. Dados brutos entram na camada Bronze.
2. Notebook de Silver lê a Bronze, aplica schema e regras de qualidade.
3. A tabela Silver é atualizada por merge/upsert para garantir idempotência.
4. O notebook de Gold transforma a Silver em modelo estrela.
5. Dimensões e tabela fato são gravadas no catálogo Delta.
6. A ontologia e o data agent consomem as entidades com nomenclatura de negócio.

## Decisões de Arquitetura
- Schema explícito em vez de inferência.
- Uso de nomes qualificados por catálogo para evitar objetos não identificados.
- Uso de merge/upsert do Delta para recargas seguras.
- Modelo em estrela para relatórios e consumo semântico.
- Alinhamento entre a ontologia e a nomenclatura de fatos e dimensões.

## Segurança e Governança
- Manter dados sensíveis fora de logs e saídas públicas.
- Padronizar nomes e descrições de colunas para apoio da ontologia.
- Registrar regras de qualidade e o motivo de sua existência.

## Estratégia de Testes
- Validação de contagem de linhas por camada.
- Verificação de nulos e duplicatas.
- Teste de integridade da chave de negócio na tabela fato.
- Verificação da chave DateKey e relacionamento com as dimensões.
- Reprocessamento de dados para validar idempotência.

## Riscos e Mitigações
- Risco: inferência de tipos incorreta na ingestão. Mitigação: schema explícito.
- Risco: duplicação de registros. Mitigação: merge por chave de negócio.
- Risco: desalinhamento de semântica entre ontologia e modelo. Mitigação: governança de nomes e mapeamentos.
- Risco: dependência de nomes de tabela em ambiente específico. Mitigação: registro no catálogo e uso de nomes qualificados.

## Entregáveis
- Pipeline Bronze → Silver → Gold funcional.
- Modelo dimensional de vendas pronto para exploração.
- Ontologia e contexto para o data agent.
- Documentação mínima de operação e governança do domínio.
