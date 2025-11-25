# 🛡️ KAURA-PROJETO: Pipeline de Governança e Inteligência de Documentos (IDP)

## ✨ Proposta de Valor KAURA: Inovação Centrada no Ser Humano

> 🧠 **O Problema Humano:** A digitalização manual de dados de faturas, contratos e outros documentos é uma tarefa **tediosa e repetitiva**, desviando o foco dos colaboradores de atividades que exigem **julgamento humano, empatia e tomada de decisão estratégica**.
>
> **A Solução KAURA:** Este projeto é a prova de conceito (PoC) de um serviço de consultoria em IA e dados que atua como um **"Filtro Inteligente"**. Ele automatiza a Extração e a Transformação de documentos (IDP), liberando o tempo do colaborador para o que realmente importa.

## 🎯 Domínio e Contexto Estratégico
O pipeline foi desenhado com foco no atendimento a **PMEs (Pequenas e Médias Empresas)** com atuação no mercado europeu, onde a **conformidade regulatória (RGPD/GDPR)** é crítica. Nossa camada de **Transformação** é reforçada com um módulo de **Anonimização**, garantindo que os dados pessoais sensíveis sejam protegidos.

## ⚙️ Arquitetura Profissional (Orquestração e Governança)

O pipeline implementa uma arquitetura completa, orquestrada com o **Framework Luigi**, que assegura a dependência, resiliência e a visualização do fluxo de trabalho.

| Fase | Ferramenta/Conceito | Descrição com Foco na Governança e IA |
| :--- | :--- | :--- |
| **E - Extração** | **JSON Simulado (Input)** | Representa a saída de um serviço de OCR/IDP, como o **Azure Document Intelligence** ou AWS Textract, que converte faturas em dados estruturados brutos. |
| **T - Transformação** | **Pandas & Módulo RGPD** | **Limpeza:** Tratamento de nulos e tipagem. **Governança (KAURA):** Injeção de uma função de **Anonimização/Pseudonimização** de campos sensíveis (e.g., NIF/NISS), garantindo o *compliance* com o RGPD/GDPR. |
| **L - Carga (Load)** | **Luigi (Orquestrador) / CSV** | Utiliza o **Luigi** para gerenciar a execução de forma robusta e persistir o resultado **limpo e anonimizado** em um arquivo final, simulando o carregamento para um Data Warehouse (DW) seguro. |

## 🛠️ Tecnologias de Destaque
* **Python (3.x)**
* **Pandas:** Para manipulação de dados.
* **Luigi:** Framework de Orquestração de Jobs em Python (Permite visualização do fluxo).
* **Conceitos de RGPD/GDPR:** Aplicação prática da ética e governança de dados na fase de Transformação.

---
## 👩‍💻 Mentoria e Contato

> Este projeto é parte da evolução de um portfólio de **Consultoria em IA e Data Governance**. Para dúvidas sobre a arquitetura, aplicação em negócios (PMEs) ou a camada de Governança RGPD, entre em contato.
