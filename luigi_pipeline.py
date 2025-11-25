# ==============================================================================
# Fase 2: Estrutura Luigi (Extração)
# No Luigi, o pipeline não é um único script, mas sim uma coleção de Tarefas (Tasks) que dependem umas das outras. Vamos começar com a Extração.
# ==============================================================================

import luigi
import luigi.contrib.local_file # Para o Target (onde a saida sera salva)
import pandas as pd
import json

# Define a localização do arquivo de dados brutos (entrada)
RAW_DATA_PATH = 'data/faturas_raw.json'
# Define a localização do arquivo da Fase E (saída)
EXTRACT_OUTPUT_PATH = 'data/faturas_extracted.csv'

# ==============================================================================
# TAREFA 1: Extração (E)
# ==============================================================================
class ExtractTask(luigi.Task):
    """
    Extrai dados do JSON bruto e salva em um arquivo CSV temporário.
    """
    
    # Método obrigatório: define onde a saída da tarefa será salva.
    def output(self):
        """Define o arquivo de saída desta tarefa."""
        # luigi.LocalTarget é o alvo mais comum (um arquivo local)
        return luigi.LocalTarget(EXTRACT_OUTPUT_PATH)

    # Método obrigatório: contém a lógica real da tarefa.
    def run(self):
        """Executa a lógica de extração."""
        
        print("⏳ Iniciando Tarefa: Extração de Dados (Fase E)")
        
        try:
            with open(RAW_DATA_PATH, 'r') as f:
                data = json.load(f)
            df = pd.DataFrame(data)

            # Salva o DataFrame extraído como CSV.
            # O Luigi verifica a existência deste arquivo em 'output()'
            df.to_csv(self.output().path, index=False)
            
            print(f"✅ Extração Concluída! Dados salvos em: {self.output().path}")

        except FileNotFoundError:
            raise Exception(f"Erro: Arquivo {RAW_DATA_PATH} não encontrado.")


# Próxima Ação: Vamos rodar a primeira tarefa!
