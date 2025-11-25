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
TRANSFORM_OUTPUT_PATH = 'data/faturas_processed.csv'

# ==============================================================================
# TAREFA 1: Extração (E)
# ==============================================================================
class ExtractTask(luigi.Task):
    """
    Extrai os dados do JSON e salva como um CSV limpo.
    """
    def output(self):
        # Usa luigi.LocalTarget, a classe padrão para arquivos locais.
        return luigi.LocalTarget(EXTRACT_OUTPUT_PATH) 

    def run(self):
        print("⏳ Iniciando Tarefa: Extração de Dados (Fase E)")
        
        # Leitura do arquivo JSON bruto
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        df = pd.DataFrame(data)
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Extração Concluída! Dados brutos salvos em: {self.output().path}")


# ==============================================================================
# TAREFA 2: Transformação (T) - Aplicação da Governança (RGPD/KAURA ID)
# ==============================================================================
class TransformTask(luigi.Task):
    """
    Carrega dados do ExtractTask, aplica a transformação (RGPD) e salva o resultado.
    """
    def requires(self):
        """Depende da Extração."""
        return ExtractTask()

    def output(self):
        """Define o arquivo de saída desta tarefa."""
        return luigi.LocalTarget(TRANSFORM_OUTPUT_PATH)

    def run(self):
        print("⏳ Iniciando Tarefa: Transformação de Dados (Fase T)")
        
        input_file = self.input().path
        df = pd.read_csv(input_file)

        # Aplicar as transformações (Limpeza e Governança RGPD)
        df.columns = ['index', 'data_emissao', 'nif_cliente', 'valor']
        df['valor'] = df['valor'].astype(str).str.replace('R$', '', regex=False).str.replace(',', '.', regex=False).astype(float)

        # Governança de Dados (RGPD - Anonimização do NIF)
        df['id_anonimo'] = 'KAURA_' + (df.index + 1).astype(str)
        df = df.drop(columns=['index', 'nif_cliente'])
        
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Transformação Concluída! Dados transformados salvos em: {self.output().path}")

# ==============================================================================
# PONTO DE EXECUÇÃO: Essencial para rodar o Luigi
# ==============================================================================
if __name__ == '__main__':
    # Esta linha faz o Luigi assumir o controle e rodar a tarefa pedida na linha de comando
    luigi.run()