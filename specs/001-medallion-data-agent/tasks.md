# Tarefas de Implementação

## Fase 1: Fundamentos do Projeto
- [ ] Revisar e consolidar o contexto do domínio de vendas
- [ ] Confirmar requisitos funcionais e não funcionais do fluxo analítico
- [ ] Registrar princípios governantes na constituição do projeto

## Fase 2: Ingestão e Bronze
- [ ] Validar arquivos de origem e schema de entrada
- [ ] Configurar camada Bronze para preservação de rastreabilidade
- [ ] Criar processo de leitura segura dos arquivos CSV

## Fase 3: Silver
- [ ] Definir regras de limpeza e enriquecimento
- [ ] Normalizar nomes, emails e datas
- [ ] Implementar deduplicação por chave de negócio
- [ ] Validar qualidade, flags e auditoria

## Fase 4: Gold
- [ ] Construir dimensão de data
- [ ] Construir dimensão de cliente
- [ ] Construir dimensão de produto
- [ ] Construir tabela fato de vendas
- [ ] Validar integridade do modelo estrela

## Fase 5: Ontologia e Semântica
- [ ] Mapear termos de negócio para entidades e propriedades
- [ ] Conectar conceitos de cliente, produto, data e venda
- [ ] Validar que o agent consegue responder perguntas em linguagem natural

## Fase 6: Qualidade e Entrega
- [ ] Executar validações de linha por camada
- [ ] Testar reprocessamento e merge/upsert
- [ ] Revisar documentação, governança e manutenção
- [ ] Preparar handoff para uso analítico e operacional
