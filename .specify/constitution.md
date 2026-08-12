# Constituição do Agente de Dados Medallion

## Princípios Fundamentais

### I. Qualidade dos Dados é Inegociável
Toda carga, transformação e alteração de modelo devem preservar correção, consistência e rastreabilidade. Contratos de dados, schemas explícitos, tratamento de nulos e validações são mandatórios antes da promoção de dados entre as camadas Bronze, Silver e Gold.

### II. Semântica de Negócio Antes da Implementação Técnica
O projeto deve priorizar o significado do negócio em vez de nomes brutos de tabelas. Conceitos de cliente, produto, data e vendas devem ser representados com linguagem clara do domínio e relacionamentos consistentes para que o agente de dados responda perguntas em linguagem natural com confiabilidade.

### III. Pipelines Idempotentes e Repetíveis
Todas as transformações devem ser repetíveis e seguras para reexecução. Upserts, lógica de merge, deduplicação e sobrescritas controladas são necessários para evitar duplicação acidental ou desvio quando as fontes forem reprocessadas.

### IV. Observabilidade e Auditabilidade
Cada etapa do pipeline deve deixar evidências suficientes para explicar o que aconteceu, de onde veio e quando foi carregado. Metadados, linhagem de arquivos, timestamps e saídas de validação fazem parte da implementação, não apenas de documentação opcional.

### V. Arquitetura Analítica Preparada para o Domínio
O padrão da solução é uma arquitetura em medalhão ancorada em um modelo dimensional adequado para análise semântica e consulta em linguagem natural. As tabelas Gold devem ser estruturadas para relatórios e governadas por chaves de negócio acordadas e chaves substitutas.

## Restrições Adicionais

### Padrões da Plataforma de Dados
- Usar Microsoft Fabric Lakehouse e tabelas Delta para armazenamento persistente.
- Preferir schemas explícitos em vez de processamento pesado em inferência.
- Usar nomes de tabela qualificados por catálogo para garantir o registro correto no metastore do lakehouse.
- Manter a ingestão bruta separada dos dados limpos e curados.

### Padrões de Qualidade
- Validar contagens de linhas e perfis de nulos após cada transformação principal.
- Rejeitar ou sinalizar registros suspeitos em vez de permitir que dados ruins entrem silenciosamente em camadas curadas.
- Definir e aplicar identidades de negócio-chave como SalesOrderNumber + SalesOrderLineNumber, CustomerName + Email e mapeamentos baseados em Item.

### Padrões de Documentação
- Cada notebook ou mudança de pipeline deve descrever origem, destino, premissas e transformações.
- Termos do glossário de negócio devem estar alinhados aos nomes da ontologia e às entidades do modelo semântico.
- Alterações de modelo devem ser acompanhadas pelo contrato de dados subjacente.

## Fluxo de Desenvolvimento

1. Os requisitos são capturados como resultados de negócio e perguntas de dados antes das mudanças de implementação.
2. Contratos de dados e semântica do domínio são revisados antes da aprovação de mudanças no pipeline.
3. Transformações são implementadas com validação explícita e verificações mensuráveis.
4. Alterações são revisadas quanto ao impacto em modelos semânticos downstream, comportamento do agente de dados e experiência do usuário.
5. Reprocessamento e hidratação devem permanecer seguros e previsíveis.

## Governança

Esta constituição governa todas as mudanças do projeto. Qualquer trabalho que enfraqueça a qualidade dos dados, reduza a rastreabilidade ou quebre o modelo semântico de negócio é considerado não conforme até ser corrigido. O projeto deve priorizar clareza, repetibilidade e confiabilidade em vez de conveniência de curto prazo.

Versão: 1.0.0 | Ratificada: 2026-08-12 | Última alteração: 2026-08-12
