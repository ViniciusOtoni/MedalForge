# MedalForge

*MedalForge* é um projeto desenvolvido no Databricks, utilizando Apache Spark, com o objetivo de automatizar e otimizar os processos das três camadas de um lakehouse seguindo o conceito da arquitetura Medalium:

### Estrutura do Projeto

 * Camada Bronze: Responsável pela ingestão e catalogação inicial dos dados.
 * Camada Silver: Automatiza o refinamento dos dados e prepara conjuntos consistentes e limpos.
 * Camada Gold: Foco na criação de datasets otimizados para análise e modelos preditivos.

    - Camada Bronze (Ingestão): Implementa um processo de ingestão automatizado para lidar com dados estruturados, semiestruturados e não estruturados. Utiliza o Design Pattern Factory para abstrair e definir esquemas de forma dinâmica.

    - Camada Silver (Transformação): Automatiza tarefas de ETL, como remoção de duplicatas, tratamento de valores nulos e pré-processamento avançado (ex.: lematização). Baseia-se no Design Pattern Strategy para modularizar diferentes estratégias de transformação.

    - Camada Gold (Feature Store): Finaliza o pipeline criando uma Feature Store otimizada para machine learning e analytics, também empregando o Design Pattern Strategy para criar pipelines customizados para geração de features.

Todas as etapas são orquestradas em um único Job no Databricks, com tasks separadas para cada camada, garantindo automação, escalabilidade e facilidade de manutenção.


### Principais Tecnologias:
 - Databricks
 - Apache Spark
 - Arquitetura Lakehouse
 - Python
 - Design Patterns (Factory, Strategy)


### Objetivos:

1. Automatizar os fluxos de dados para ingestão, transformação e geração de features.
2. Garantir modularidade e reusabilidade do código.
3. Facilitar o processamento de grandes volumes de dados em diferentes formatos.
4. Proporcionar insights e features de alta qualidade para uso em projetos de Machine Learning e BI.


### Por que "MedalForge"?
O nome reflete o propósito do projeto: "forjar" dados brutos (Bronze) em informações refinadas e valiosas (Gold), respeitando as etapas intermediárias de transformação e limpeza (Silver), com a eficiência de um processo automatizado.