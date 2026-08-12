# Especificação do Projeto

## Visão Geral
Este projeto implementa um fluxo de dados em arquitetura medalhão para vendas, com foco em tornar o domínio de negócio acessível a usuários finais por meio de um data agent. A solução organiza os dados em Bronze, Silver e Gold, transforma os dados em um modelo dimensional e expõe semântica clara para perguntas em linguagem natural.

## Problema a Resolver
A organização precisa responder perguntas de negócio sobre vendas, clientes, produtos e calendário sem depender de consultas técnicas complexas ou conhecimento de esquema de banco. Também é necessário manter um pipeline de dados confiável, reprocessável e fácil de auditar.

## Objetivo do Produto
Disponibilizar uma base analítica estruturada para dar suporte a relatórios, exploração analítica e interação com um agente conversacional sobre desempenho comercial.

## Personas e Usuários
- Analista de vendas
- Gerente de negócio
- Data engineer
- Usuário final que consulta dados em linguagem natural

## Requisitos Funcionais

### RF-01: Ingestão em Bronze
O sistema deve receber arquivos de origem em camada Bronze sem perder rastreabilidade do arquivo original.

### RF-02: Padronização em Silver
Os dados de pedidos devem ser limpos, normalizados e deduplicados na camada Silver, com regras explícitas para nomes, emails, datas e flags de qualidade.

### RF-03: Modelo dimensional em Gold
A camada Gold deve criar dimensões de data, cliente e produto e uma tabela fato de vendas com chave de negócio e métricas de valor.

### RF-04: Relações semânticas
As entidades do domínio devem ter nomes e relações consistentes para que o data agent interprete corretamente termos como cliente, produto, pedido, data e faturamento.

### RF-05: Consultas em linguagem natural
O data agent deve responder perguntas sobre faturamento, quantidade, clientes, produtos e períodos usando a ontologia e o modelo semântico.

### RF-06: Reprocessamento seguro
O fluxo deve suportar recarga e reprocessamento sem duplicar registros ou corruptar o histórico analítico.

## Requisitos Não Funcionais

### RNF-01: Performance
O pipeline deve processar volumes de dados analíticos em tempo compatível com cargas diárias e regionais.

### RNF-02: Qualidade de dados
O sistema deve validar valores nulos, duplicações e inconsistências antes de avançar para camadas posteriores.

### RNF-03: Observabilidade
Toda etapa deve registrar informações suficientes para auditoria e diagnóstico.

### RNF-04: Manutenibilidade
As transformações devem ser legíveis, documentadas e organizadas por camada.

## Critérios de Aceitação
- A camada Bronze recebe arquivos brutos sem perda de contexto.
- A camada Silver remove duplicações e normaliza os principais campos.
- A camada Gold produz um star schema funcional.
- O data agent entende termos de negócio e responde perguntas em linguagem natural.
- Reexecutar a pipeline não gera duplicidade de registros.

## Restrições
- A solução deve usar Microsoft Fabric e OneLake como base de implementação.
- O modelo deve respeitar o padrão de Lakehouse e metastore do ambiente.
- O projeto é orientado a demonstração e educação de arquitetura analítica.

## Escopo Fora do Escopo
- Processamento em tempo real em streaming
- Múltiplos domínios além de vendas
- Integração com outros painéis e canais de consumo além do data agent/semântico
