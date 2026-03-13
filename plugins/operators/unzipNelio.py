from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import zipfile

class UnzipFilesOperator(BaseOperator):
    """
    Operator customizado para descompactar múltiplos arquivos .zip em um diretório.
    """
    # Definimos quais campos podem receber Jinja templates (útil para passar datas dinâmicas)
    template_fields = ("zip_path", "file_save") 

    def __init__(self, zip_path: str, file_save: str, **kwargs):
        super().__init__(**kwargs)
        self.zip_path = zip_path
        self.file_save = file_save

    def execute(self, context):
        self.log.info(f"Iniciando descompactação de: {self.zip_path}")
        
        # Criar o diretório de destino se não existir
        if not os.path.exists(self.file_save):
            os.makedirs(self.file_save)
            self.log.info(f"Diretório criado: {self.file_save}")

        for file_name in os.listdir(self.zip_path):
            if file_name.endswith('.zip'):
                full_path = os.path.join(self.zip_path, file_name)
                
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        zip_ref.extract(member, self.file_save)
                        self.log.info(f"Extraído: {member} para {self.file_save}")
        
        return self.file_save # Útil para a próxima task saber onde os arquivos estão