olist <- arrow::read_parquet(
    "https://github.com/padsInsper/202533-padsv/releases/download/dados/olist_items.parquet"
)

dplyr::glimpse(olist)

# types

dados_grafico <- olist |>
    dplyr::count(types) |>
    dplyr::filter(n > 100) |>
    dplyr::mutate(n = n / 1000)

library(ggplot2)

dados_grafico |>
    ggplot(aes(x = n, y = types)) +
    geom_col(fill = "#8ae3d7", width = .5) +
    geom_label(aes(label = round(n, 2), x = n / 2)) +
    theme_dark(16) +
    labs(
        x = "Quantidade\n(milhares)",
        y = "Forma de pagamento",
        title = "Formas de pagamento mais comuns",
        subtitle = "Considerando tipos com mais de 100 observações",
        caption = "Fonte: Olist"
    ) +
    theme(
        panel.background = element_rect(fill = "gray20"),
        plot.background = element_rect(fill = "gray10"),
        text = element_text(family = "serif", colour = "white"),
        axis.text = element_text(family = "serif", colour = "white")
    )


ggplot()
