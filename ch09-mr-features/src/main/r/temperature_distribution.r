png("temperature_distribution.png")
data <- read.table("output_sorted")
plot(data, xlab="Temperatura", ylab="Liczba odczytów")
dev.off()