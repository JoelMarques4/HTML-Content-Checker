import asyncio
import aiohttp
import pandas as pd
import customtkinter as ctk
from tkinter import filedialog
from bs4 import BeautifulSoup
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BATCH_SIZE = 50
RETRY_COUNT = 3
TIMEOUT = 30

ctk.set_appearance_mode("dark")  
ctk.set_default_color_theme("blue")


async def fetch_product_page(
    session: aiohttp.ClientSession, 
    base_url: str, 
    code: str, 
    option: str, 
    retries: int = RETRY_COUNT
) -> tuple:
    """
    Fetches the product page for a given SKU and checks for its content.

    Parameters:
    - session: An aiohttp.ClientSession object used to make HTTP requests.
    - base_url: The base URL of the website to search for products.
    - code: The SKU code of the product to search for.
    - option: A string indicating the source option, e.g., "efacil".
    - retries: Optional integer specifying the number of retry attempts for fetching the page.

    Returns:
    - A tuple containing the SKU code, the search URL, and a status message ("Sim", "Não", or "Erro").

    If the product link is found and the content check is successful, it returns "Sim". 
    Otherwise, it returns "Não" or "Erro" depending on the failure point.

    Logs warnings and errors during the fetching process.
    """

    if option == "efacil":
        search_url = f"{base_url}/loja/busca/?searchTerm={code}"

    for attempt in range(retries):
        try:
            async with session.get(search_url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    if option == "efacil":
                        link_tag = soup.find('a', id=f'btn_skuP{code}')
                    else:
                        return code, search_url, "Erro"
                    
                    if link_tag and link_tag.has_attr('href'):
                        product_url = base_url + link_tag['href']
                        return await check_product_content(session, product_url, code)
                    else:
                        logging.warning(f"Nenhum link de produto foi encontrado para o SKU {code} no {option}.")
                        return code, search_url, "Erro"
        except asyncio.TimeoutError:
            logging.warning(f"Timeout ao buscar SKU {code} na tentativa {attempt + 1}.")
        except Exception as e:
            logging.error(f"Tentativa {attempt + 1} falha na busca {search_url}: {e}")
        await asyncio.sleep(1)
    return code, search_url, "Erro"

async def check_product_content(
    session: aiohttp.ClientSession, 
    product_url: str, 
    code: str
) -> tuple:
    """
    Checks if the product page contains content.

    Parameters:
    - session: An aiohttp.ClientSession object used to make HTTP requests.
    - product_url: The URL of the product page to check.
    - code: The SKU code of the product.

    Returns:
    - A tuple containing the SKU code, the product URL, and a status message ("Sim" or "Não").

    Logs warnings and errors during the content check process.
    """

    try:
        async with session.get(product_url, timeout=TIMEOUT) as response:
            if response.status == 200:
                html = await response.text()
                return code, product_url, "Sim" if 'lp-container' in html else "Não"
    except asyncio.TimeoutError:
        logging.warning(f"Timeout ao verificar a página do produto {product_url}.")
    except Exception as e:
        logging.error(f"Falha ao verificar a página do produto {product_url}: {e}")
    return code, product_url, "Erro"

async def process_skus(
    option: str, 
    codes: list, 
    progress_callback: callable
) -> list:
    """
    Processes a list of SKUs and checks for their content.

    Parameters:
    - option: A string indicating the source option, e.g., "efacil".
    - codes: A list of SKU codes to process.
    - progress_callback: A callback function to update the progress bar.

    Returns:
    - A list of tuples containing the SKU code, the search URL, and a status message ("Sim", "Não", or "Erro").

    Logs warnings and errors during the processing.
    """

    results = []
    base_urls = {
        "martins": "https://www.martinsatacado.com.br",
        "efacil": "https://www.efacil.com.br"
    }
    base_url = base_urls.get(option, "")
    if not base_url:
        return []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i, code in enumerate(codes):
            tasks.append(fetch_product_page(session, base_url, code, option))
            if len(tasks) >= BATCH_SIZE or i == len(codes) - 1:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    progress_callback(i + 1)
                tasks.clear()
                
    return results

class URLChecker(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Checar Conteúdo")
        self.geometry("500x300")
        self.resizable(False, False)
        self.iconbitmap("html-icon.ico")

        # UI Elements
        self.file_label = ctk.CTkLabel(self, text="Selecione o arquivo Excel:")
        self.file_label.pack(pady=5)

        self.file_button = ctk.CTkButton(self, text="Selecionar Arquivo", command=self.select_file)
        self.file_button.pack(pady=5)

        self.selected_file_label = ctk.CTkLabel(self, text="", fg_color="gray20", width=400, height=25)
        self.selected_file_label.pack(pady=5)

        self.option_var = ctk.StringVar(value="efacil")

        self.progress_bar = ctk.CTkProgressBar(self, mode="determinate", width=300)
        self.progress_bar.set(0)
        self.progress_bar.pack(pady=10)

        self.process_button = ctk.CTkButton(self, text="Iniciar Processamento", command=self.process_file)
        self.process_button.pack(pady=10)

        self.result_label = ctk.CTkLabel(self, text="", fg_color="gray20", width=400, height=25)
        self.result_label.pack(pady=10)

        self.credits_label = ctk.CTkLabel(self, text="Creditos: Joel Silveira Marques Pereira \n LinkedIn: Joel Silveira Marques Pereira")
        self.credits_label.pack(pady=2)

    def select_file(self) -> None:
        """
        Opens a file dialog to select an Excel file.
        """

        file_path = filedialog.askopenfilename(filetypes=[["Excel files", "*.xlsx"]])
        if file_path:
            self.selected_file_label.configure(text=file_path)

    def save_file(self) -> str:
        """
        Opens a file dialog to save the result as an Excel file.

        Returns:
        - The path of the saved file.
        """

        return filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[["Excel files", "*.xlsx"]])

    def update_progress_bar(self, current: int, total: int) -> None:
        """
        Updates the progress bar with the current progress.

        Parameters:
        - current: The current progress (0 <= current < total).
        - total: The total number of tasks.
        """

        self.progress_bar.set(current / total)
        self.update_idletasks()

    def process_file(self) -> None:
        """
        Processes the selected Excel file and checks for the content of the products.
        """

        input_file = self.selected_file_label.cget("text")
        if not input_file:
            self.result_label.configure(text="Por favor, selecione um arquivo Excel.")
            return

        option = self.option_var.get()
        df = pd.read_excel(input_file)
        if 'SKU' not in df.columns:
            self.result_label.configure(text="O arquivo Excel deve conter uma coluna chamada 'SKU'.")
            return

        df['SKU'] = df['SKU'].astype(str).str.strip()
        codes = df['SKU'].tolist()
        total = len(codes)

        def progress_callback(current):
            self.update_progress_bar(current, total)

        async def run_processing():
            try:
                results = await process_skus(option, codes, progress_callback)
                results_df = pd.DataFrame(results, columns=["SKU", "URL", "Tem Conteúdo"])
                save_path = self.save_file()
                if save_path:
                    results_df.to_excel(save_path, index=False)
                    self.result_label.configure(text=f"Resultados salvos em {save_path}.")
                    os.startfile(save_path)
            except Exception as e:
                self.result_label.configure(text=f"Erro: {e}")
                logging.error(f"Erro de processamento: {e}")

        asyncio.run(run_processing())

if __name__ == "__main__":
    app = URLChecker()
    app.mainloop()

