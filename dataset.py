# Data Analysis Libraries
import pandas as pd
import math
import string
import time

# Web Scrapping Libraries
import requests
from bs4 import BeautifulSoup

import shutil
import pickle
from pathlib import Path

# pd.set_option('display.max_colwidth', None)
pd.set_option("display.max_columns", None)

def salva_csv(livros: list):
    """Salva o DataFrame em um arquivo csv"""
    df_livros = pd.DataFrame(livros)
    df_livros.dropna(subset=["reviews"]).numero_do_livro.nunique()

    auxiliar_resenhas = pd.DataFrame(
        columns=[
            "review_id",
            "user_id",
            "numero_do_livro",
            "pagina_do_skoob",
            "posicao_na_pagina",
            "rating",
            "review_title",
            "review",
        ]
    )


    for i, d in df_livros.reviews.iteritems():
        auxiliar_resenhas = pd.concat(
            [auxiliar_resenhas, pd.DataFrame(d)], ignore_index=True
        )

    df = df_livros.drop("reviews", axis=1).merge(
        auxiliar_resenhas, on="numero_do_livro", how="inner"
    )

    df.to_csv("df_livros_10001_20000.csv")

def cria_backup():
    """Cria um backup do arquivo csv"""
    try:
        shutil.copyfile("df_livros_10001_20000.csv", "df_livros_10001_20000_backup.csv")
    except Exception:
        pass

def existe_arquivo_estado():
    """Verifica se existe um arquivo de estado salvo"""
    my_file = Path("save_data.dat")
    return my_file.is_file()

def salva_estado(livros: list, contagem: int, i: int):
    """Salva o estado atual do processo"""
    d = {"livros": livros, "contagem": contagem, "i": i}
    with open("save_data.dat", "wb") as f:
        pickle.dump(d, f)

def carrega_estado():
    """Carrega o estado salvo"""
    with open("save_data.dat", "rb") as f:
        d = pickle.load(f)
    return d["livros"], d["contagem"], d["i"]

def dados_iniciais_livro(i):

    # função para obter informações iniciais do livro utilizando a numeração de identificação do livro no site Skoob.
    # informações iniciais obtidas:
    # Livro, Autor, ISBN-13, lançamento, páginas do livro, total de resenhas, url onde se encontram as resenha

    url_resenha = (
        f"https://www.skoob.com.br/livro/resenhas/{i}/mais-gostaram/mpage:1"
    )
    # página do Skoob em que constam as resenhas, em que o primeiro {} será o número do livro e
    # o segundo {} a página de reviews

    response = requests.get(url_resenha)
    pagina_html = BeautifulSoup(response.text, "html.parser")
    corpo = pagina_html.findAll(id="corpo")[
        0
    ]  # corpo da página a ser feita a raspagem de dados (Web Scraping)
    cabecario = corpo.find(id="pg-livro-menu-principal-container")

    Livro = cabecario.find("strong", {"class": "sidebar-titulo"}).get_text()

    try:
        Autor = cabecario.findAll("a")[1].get_text()  # Nome do Autor
    except:
        Autor = None  # Caso não obtenha nome de autor
    if (
        Autor == "\nR$ \n"
    ):  # nos casos em que o nome do autor não possui um hiperlink, ele não será encontrado com Find('a')
        Autor = cabecario.find(
            "i"
        ).get_text()  # nestes casos, um find i seria necessário
    elif Autor == "\n\n":  # outra opção de resultado para casos em que não há hiperlink
        Autor = cabecario.find("i").get_text()

    try:
        ISBN_13 = (
            cabecario.find("div", {"class": "sidebar-desc"}).find("span").get_text()
        )  # obter ISBN do livro
    except:
        ISBN_13 = None

    try:
        lancamento = int(
            cabecario.find("div", {"class": "sidebar-desc"})
            .get_text()
            .split("Ano: ")[-1]
            .split(" /", 1)[0]
        )
    except:
        lancamento = None

    try:
        Paginas = int(
            cabecario.find("div", {"class": "sidebar-desc"})
            .get_text()
            .split("Páginas: ")[-1]
            .split(" ", 1)[0]
        )
    except:
        Paginas = None

    try:  # obter o total de resenhas disponíveis, para identificar o range necessário para obter todas as resenhas
        total_resenhas = int(
            corpo.find("div", {"class": "contador"}).find("b").get_text().split(" ")[0]
        )
    except:
        total_resenhas = 0

    print("Livro {}: {}".format(i, Livro))

    return (Livro, Autor, ISBN_13, lancamento, Paginas, total_resenhas, url_resenha)


def detalhes_do_livro(i, Livro):

    # função para obter um maior detalhamento do livro, com informações do skoob
    # Descricao, gêneros, Rating médio, Avaliações, total de resenhas, usuários que abandonaram a leitura
    # usuários relendo, usuários que querem_ler, usuários lendo, usuários que leram, url do detalhe do livro

    titulo_tratado = Livro  # tratar o titulo, retirando pontuações dos titulos, que não são utilizados no link
    for c in string.punctuation:
        titulo_tratado = titulo_tratado.replace(c, "")

    # link dos detalhes de cada livro é composto pelo titulo, com um hifem entre palavras
    # e pelo numero de identificação do livro no site
    url_livro = (
        (f"https://www.skoob.com.br/{titulo_tratado}-{i}")
        .replace(" ", "-")
        .lower()
        .replace("--", "-")
    )

    response2 = requests.get(url_livro)
    pagina_html2 = BeautifulSoup(response2.text, "html.parser")
    corpo2 = pagina_html2.find(
        id="corpo"
    )  # corpo da página a ser feita a raspagem de dados (Web Scraping)
    detalhes = corpo2.find(id="pg-livro-principal-container")

    Rating = float(detalhes.find(id="pg-livro-box-rating").find("span").get_text())
    Avaliacoes = int(
        detalhes.find(id="pg-livro-box-rating-avaliadores-numero")
        .get_text()
        .split(" ")[0]
        .replace(".", "")
    )

    total_resenhas_real = int(
        detalhes.findAll("div", {"class": "bar"})[0]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )
    abandonos = int(
        detalhes.findAll("div", {"class": "bar"})[1]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )
    relendo = int(
        detalhes.findAll("div", {"class": "bar"})[2]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )
    querem_ler = int(
        detalhes.findAll("div", {"class": "bar"})[3]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )
    lendo = int(
        detalhes.findAll("div", {"class": "bar"})[4]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )
    leram = int(
        detalhes.findAll("div", {"class": "bar"})[5]
        .findAll("a")[1]
        .get_text()
        .replace(".", "")
    )

    try:
        descricao = detalhes.find(id="livro-perfil-sinopse-txt").find("p").get_text()
    except:
        descricao = None

    try:  # procurar encontrar um gênero, caso exista, encontar a descrição
        generos = detalhes.find(id="livro-perfil-sinopse-txt").find("span").get_text()
        descricao = descricao.split(generos)[0]  # remover generos da descrição
        generos = generos.strip().split(
            " / "
        )  # gerar uma lista com os diversos gêneros
    except:
        generos = None

    return (
        descricao,
        generos,
        Rating,
        Avaliacoes,
        total_resenhas_real,
        abandonos,
        relendo,
        querem_ler,
        lendo,
        leram,
        url_livro,
    )


def data_de_lancamento(i, lancamento):

    # função para procurar a data de lançamento "correta"
    # o dado obtido anteriormente não necessariamente é a da primeira edição lançada
    # procurar dentre as diversas edições a que tiver a menor data de lançamento

    url_edicoes = f"https://www.skoob.com.br/livro/edicoes/{i}"

    response3 = requests.get(url_edicoes)
    pagina_html3 = BeautifulSoup(response3.text, "html.parser")
    corpo3 = pagina_html3.find(id="corpo").find(
        "div", {"style": "margin-top:10px;"}
    )  # corpo da página a ser feita a raspagem de dados (Web Scraping)

    numero_edicoes = len(corpo3.findAll("div", {"style": "float:left; width:180px;"}))
    lancamentos = []
    for edicoes in range(0, numero_edicoes):
        lancamentos.append(
            int(
                corpo3.findAll("div", {"style": "float:left; width:180px;"})[edicoes]
                .get_text()
                .split("Ano: ")[-1]
                .split("Páginas:")[0]
            )
        )
    try:
        if (
            lancamento == None
        ):  # caso não houvesse dado disponível, obter o mínimo existente
            try:
                lancamento = min(lancamentos)
            except:
                pass
        elif (
            min(lancamentos) < lancamento
        ):  # caso o menor valor disponível seja menor que o atual, substituir
            lancamento = min(lancamentos)
    except:
        pass

    return lancamento


def obter_resenhas(i, Livro, total_resenhas, limite_resenhas, imprimir):

    lista_reviews = []  # lista com output de todas as reviews coletadas

    url_resenha = "https://www.skoob.com.br/livro/resenhas/{}/mais-gostaram/mpage:{}"

    total_pags = math.ceil(total_resenhas / 15)

    if (
        limite_resenhas == 0
    ):  # se limite for definido como 0, pegará todas os comentários existentes
        pass
    elif total_pags > math.ceil(
        limite_resenhas / 15
    ):  # se houverem mais páginas que as necessárias para encontrar o limite
        total_pags = math.ceil(
            limite_resenhas / 15
        )  # limitará as páginas de modo a limitar o total de comentários
    else:
        pass  # se limite_resenhas não for 0 nem maior que o existente, teremos menos reviews que o desejado, portanto pegaremos todos os disponíveis

    contador_reviews = 0  # definindo um contador de comentários por livro

    for paginas_de_resenha in range(
        1, total_pags + 1
    ):  # range das páginas com comentários, seguindo limite caso tenha sido definido
        response4 = requests.get(url_resenha.format(i, paginas_de_resenha))
        pagina_html4 = BeautifulSoup(response4.text, "html.parser")
        corpo4 = pagina_html4.find(id="corpo")  # corpo das reviews de cada página

        if imprimir:  # se True, irá printar o livro e página que está sendo coletado
            print(
                "Livro {}: {} - Página: {}".format(i, Livro, paginas_de_resenha)
            )  # printando página da review, para identificar casos de bug

        # existem até 15 reviews por página, cada uma com o ID da review aparecendo 2x, portanto
        # o range padrão de coleta de reviews por página vai de 0 a 29, pulando de 2 em 2
        # no entanto, possívelmente ele foi limitado

        if (
            paginas_de_resenha == math.ceil(limite_resenhas / 15)
            and limite_resenhas != 0
        ):  # se a página atual for a mesma definida ao limitar o numero de reviews
            ultima_review = (
                limite_resenhas - contador_reviews
            ) * 2  # encontrar quantas reviews ainda serão coletadas para atingir limite definido
            # multiplicar por 2 uma vez que pularemos de 2 em 2
        else:
            ultima_review = 30  # se nao estiver no limite definido, usará o range completo das reviews

        for resenhas in range(0, ultima_review, 2):
            try:  # testar se existe reviews nesta posição, uma vez que se o total não tiver sido limitado,
                # na última página de reviews pode haver menos de 15 reviews
                if (
                    imprimir
                ):  # se True, irá printar o livro e página e review que está sendo coletado
                    print(
                        "Livro {}: {} - Página: {} - Review: {}".format(
                            i, Livro, paginas_de_resenha, int(resenhas / 2 + 1)
                        )
                    )  # printando a posição da review, para identificar casos de bug

                # dados gerais da resenha
                review_pt1 = corpo4.findAll(id="perfil-conteudo-intern")[0].findAll(
                    "div", id=lambda value: value and value.startswith("resenha")
                )[resenhas]
                id_resenha = review_pt1.get("id")  # ID da review
                id_usuario = (
                    review_pt1.find("a").get("href").split("/")[-1]
                )  # ID do usuário
                nota = float(
                    review_pt1.find("star-rating").get("rate")
                )  # Nota dada pelo usuário

                # dados de texto da resenha
                review_pt2 = corpo4.find(id="perfil-conteudo-intern").findAll(
                    "div", id=lambda value: value and value.startswith("resenha")
                )[resenhas + 1]

                resenha = (
                    review_pt2.get_text()
                )  # obtendo o texto que consta título, data e resenha

                if "site: " in resenha:
                    if len(review_pt2.findAll("strong")) == 2:
                        titulo = None
                        resenha = resenha.split("/", 2)[-1][4:]
                        resenha = resenha.split("site: ")[0]
                    elif len(review_pt2.findAll("strong")) == 3:
                        titulo = review_pt2.findAll("strong")[1].get_text()
                        resenha = resenha.split(titulo, 1)[-1]
                        resenha = resenha.split("site: ")[0]
                else:
                    try:  # caso exista um título, salvar ele separadamente
                        titulo = review_pt2.findAll("strong")[1].get_text()
                        resenha = resenha.split(titulo, 1)[-1]
                    except IndexError:  # caso não exista um título, salvar None
                        titulo = None
                        resenha = resenha.split("/", 2)[-1][4:]
                if resenha == " ":
                    resenha = titulo
                    titulo = None

                contador_reviews += 1

                lista_reviews.append(
                    {
                        "numero_do_livro": i,
                        "review_id": id_resenha,
                        "user_id": id_usuario,
                        "pagina_do_skoob": paginas_de_resenha,
                        "posicao_na_pagina": (resenhas / 2 + 1),
                        "rating": nota,
                        "review_title": titulo,
                        "review": resenha,
                    }
                )
            except:
                break

    return lista_reviews


def raspar_skoob(livro_inicial, livro_final, limite_resenhas, imprimir):
    # livro_inicial: primeiro livro a ser coletado (número inteiro não nulo)
    # livro_final: ultimo livro a ser coletado, incluindo-o (sendo necessáriamente um número maior que o do livro_inicial)
    # limite_resenhas: limite de reviews a serem coletados.
    # Caso o valor seja menor que o número de reviews existentes, ele limitárá o total
    # Caso insira um valor maior que o número de reviews disponíveis, ele pegará o máximo existente
    # Caso não deseje limitar, inserir o valor 0
    # imprimir: se True, irá printar os estágios, se False, não irá printar os estágios

    print(time.asctime(time.localtime(time.time())))
    print("\n")
    if existe_arquivo_estado():
        lista_livros, contagem, livro_inicial = carrega_estado()
    else:
        contagem = 0  # contagem para definir saída do loop caso não existam 15 livros em sequência, um indício de que chegou ao final dos livros existentes
        lista_livros = []  # lista dos livros coletados

    # range da lista de livros, começando no livro_inicial até livro_final, em que será feita a coleta
    for i in range(livro_inicial, livro_final + 1):
        salva_estado(lista_livros, contagem, i)
        cria_backup()
        if contagem == 15:
            print(
                "\nHá {} tentativas não foi encontrado um livro, portanto, scraping será interrompido!".format(
                    contagem
                )
            )
            break

        while (
            True
        ):  # enquanto for true, seguira tentando obter os dados daquele único livro i
            try:  # testar se existe um livro naquela posição, caso não exista simplesmente passará para o próximo livro
                lista_reviews = []
                contagem = 0  # se o try funcionar, foi encontrado um livro, portanto, zerar contador

                # chamar a função dados_iniciais_livro
                (
                    Livro,
                    Autor,
                    ISBN_13,
                    lancamento,
                    Paginas,
                    total_resenhas,
                    url_resenha,
                ) = dados_iniciais_livro(i)

                # chamar a função detalhes_do_livro
                (
                    descricao,
                    generos,
                    Rating,
                    Avaliacoes,
                    total_resenhas_real,
                    abandonos,
                    relendo,
                    querem_ler,
                    lendo,
                    leram,
                    url_livro,
                ) = detalhes_do_livro(i, Livro)

                # chamar a função da data_de_lancamento
                lancamento = data_de_lancamento(i, lancamento)

                if total_resenhas != 0:  # caso existam resenhas para este livro
                    lista_reviews = obter_resenhas(
                        i, Livro, total_resenhas, limite_resenhas, imprimir
                    )

                else:  # caso não exista nenhuma resenha
                    lista_reviews = None
                # Salvar o livro após coleta
                lista_livros.append(
                    {
                        "autor": Autor,
                        "titulo": Livro,
                        "numero_do_livro": i,
                        "generos": generos,
                        "data_lancamento": lancamento,
                        "ISBN_13": ISBN_13,
                        "paginas": Paginas,
                        "nota_media": Rating,
                        "total_de_avaliacoes": Avaliacoes,
                        "leram": leram,
                        "lendo": lendo,
                        "querem_ler": querem_ler,
                        "relendo": relendo,
                        "abandonos": abandonos,
                        "total_resenhas": total_resenhas_real,
                        "url_livro": url_livro,
                        "url_resenha": url_resenha,
                        "descricao": descricao,
                        "reviews": lista_reviews,
                    }
                )
                salva_csv(lista_livros)
                break

            except IndexError:  # caso não exista um livro nesta página, passar para próximo livro
                contagem += 1  # adicionar 1 ao contador para cada livro não existente
                break

            except requests.exceptions.ConnectionError:  # caso dê erro de internet, continuar tentando até a internet voltar
                continue
            except requests.exceptions.ChunkedEncodingError:  # caso dê erro de internet, continuar tentando até a internet voltar
                continue
            except AttributeError:
                contagem += 1
                break
    print("\n")
    print(time.asctime(time.localtime(time.time())))
    return lista_livros

# Aqui é mantido o mesmo comportamento de antes, porém encapsulado em uma função para reutilitizar
livros = raspar_skoob(10001, 20000, 0, False)
salva_csv(livros)