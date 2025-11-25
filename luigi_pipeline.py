# ==============================================================================
# Fase 2: Estrutura Luigi (Extração)
# No Luigi, o pipeline não é um único script, mas sim uma coleção de Tarefas (Tasks) que dependem umas das outras. Vamos começar com a Extração.
# ==============================================================================
import luigi
import pandas as pd
import json

# Definições de Caminho
RAW_DATA_PATH = 'data/faturas_raw.json'
EXTRACT_OUTPUT_PATH = 'data/faturas_extracted.csv'

# ==============================================================================
# TAREFA 1: Extração (E)
# ==============================================================================
class ExtractTask(luigi.Task):
    """
    Extrai os dados do JSON e salva como um CSV limpo para a próxima fase.
    """
    # Método obrigatório: Define o destino final da saída desta tarefa.
    def output(self):
        """Define o arquivo de saída desta tarefa."""
        return luigi.LocalTarget(EXTRACT_OUTPUT_PATH)

    # Método obrigatório: Contém a lógica de extração real.
    def run(self):
        """Executa a lógica de extração."""
        
        print("⏳ Iniciando Tarefa: Extração de Dados (Fase E)")
        
        # Leitura do arquivo JSON bruto
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Criação do DataFrame com os dados
        df = pd.DataFrame(data)
        
        # Salva o resultado da extração como CSV
        # O self.output().path aponta para 'data/faturas_extracted.csv'
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Extração Concluída! Dados brutos salvos em: {self.output().path}")