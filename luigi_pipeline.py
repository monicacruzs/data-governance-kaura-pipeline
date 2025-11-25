# ==============================================================================
# Fase 2: Estrutura Luigi (Extração)
# No Luigi, o pipeline não é um único script, mas sim uma coleção de Tarefas (Tasks) que dependem umas das outras. Vamos começar com a Extração.
# ==============================================================================
import luigi
import pandas as pd
import json

# ===============================================
# Definições de Caminho: TODAS as variáveis globais
# ===============================================
RAW_DATA_PATH = 'data/faturas_raw.json'
EXTRACT_OUTPUT_PATH = 'data/faturas_extracted.csv'
TRANSFORM_OUTPUT_PATH = 'data/faturas_processed.csv'
LOAD_OUTPUT_PATH = 'data/pipeline_completo.marker' 

# ==============================================================================
# TAREFA 1: Extração (E)
# ==============================================================================
class ExtractTask(luigi.Task):
    """
    Extrai os dados do JSON e salva como um CSV limpo.
    """
    def output(self):
        return luigi.LocalTarget(EXTRACT_OUTPUT_PATH) 

    def run(self):
        print("⏳ Iniciando Tarefa: Extração de Dados (Fase E)")
        
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        df = pd.DataFrame(data)
        # CRUCIAL: index=False garante que o CSV tenha apenas as colunas de dados (3 colunas).
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Extração Concluída! Dados brutos salvos em: {self.output().path}")


# ==============================================================================
# TAREFA 2: Transformação (T) - Aplicação da Governança (RGPD/KAURA ID)
# ==============================================================================
class TransformTask(luigi.Task):
    def run(self):
        print("⏳ Iniciando Tarefa: Transformação de Dados (Fase T)")
        
        input_file = self.input()[0].path
        # O CSV LIDO AGORA possui 7 colunas (o que causava o erro)
        df = pd.read_csv(input_file)

        # *** CORREÇÃO DO ERRO 'Length mismatch' ***
        # 1. Projeta (seleciona) apenas as 3 primeiras colunas de interesse.
        # Isso reduz o DataFrame de 7 para 3 colunas, permitindo a renomeação.
        df = df.iloc[:, :3] 

        # 2. Renomeia o DataFrame que agora SÓ TEM 3 colunas.
        # Isso resolve o Length Mismatch.
        df.columns = ['data_emissao', 'nif_cliente', 'valor'] 
        # *****************************************

        # Aplicar as transformações (Limpeza e Governança RGPD)
        df['valor'] = df['valor'].astype(str).str.replace('R$', '', regex=False).str.replace(',', '.', regex=False).astype(float)

        # Governança de Dados (RGPD - Anonimização do NIF)
        df['id_anonimo'] = 'KAURA_' + (df.index + 1).astype(str)
        df = df.drop(columns=['nif_cliente']) # Remove a coluna NIF original
        
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Transformação Concluída! Dados transformados salvos em: {self.output().path}")

# ==============================================================================
# TAREFA 3: Carregamento (L)
# ==============================================================================
class LoadTask(luigi.Task):
    """
    Carrega os dados processados para o destino final (simulação de um Data Mart ou DB).
    """
    def requires(self):
        return TransformTask()

    def output(self):
        return luigi.LocalTarget(LOAD_OUTPUT_PATH)

    def run(self):
        print("⏳ Iniciando Tarefa: Carregamento de Dados (Fase L)")
        
        input_file = self.input().path
        df = pd.read_csv(input_file)
        
        # Simulação da Inserção
        # ...

        # Criação do arquivo marcador (Target)
        with self.output().open('w') as f:
            f.write(f"Pipeline KAURA concluído com sucesso. {len(df)} linhas carregadas em {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        print(f"✅ Carregamento Concluído! Dados prontos para uso. Marker criado em: {self.output().path}")

# ==============================================================================
# PONTO DE EXECUÇÃO
# ==============================================================================
if __name__ == '__main__':
    luigi.run()