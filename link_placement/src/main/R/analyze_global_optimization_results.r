.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/results/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG', Sys.getenv('HOME'))

results_chain <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_CHAIN.tsv.gz', DATADIR)),
                            header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_chain) <- paste(results_chain$src, results_chain$tgt)
results_tree <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_TREE.tsv.gz', DATADIR)),
                           header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_tree) <- paste(results_tree$src, results_tree$tgt)
results_coins <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_COINS.tsv.gz', DATADIR)),
                            header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_coins) <- paste(results_coins$src, results_coins$tgt)

col <- list(tree='red', chain='green', coins='black')

# Overlap of objectives
jaccard <- function(s1, s2) length(intersect(s1, s2)) / length(union(s1, s2))
K <- 1000
jacc_chain_coins <- sapply(1:K, function(i) jaccard(rownames(results_chain)[1:i], rownames(results_coins)[1:i]))
jacc_tree_coins <- sapply(1:K, function(i) jaccard(rownames(results_tree)[1:i], rownames(results_coins)[1:i]))
plot(jacc_tree_coins, type='l', ylim=c(0.2,1), col=col$tree)
lines(jacc_chain_coins, col=col$chain, ylab='Jaccard coefficient')
legend('bottomright', legend=c('Tree and coins', 'Chain and coins'), col=c(col$tree, col$chain), lty=1)

# Compare under chain objective
K <- 1e4
plot(cumsum(results_chain$chain_marg_gain[1:K]), type='l', col=col$chain, ylab='Value under chain objective')
lines(cumsum(results_coins$chain_marg_gain[1:K]), col=col$coins)
legend('bottomright', legend=c('Optimized for chain', 'Optimized for coins'), col=c(col$chain, col$coins), lty=1)

# Compare under tree objective
plot(cumsum(results_tree$tree_marg_gain[1:K]), type='l', col=col$tree, ylab='Value under tree objective')
lines(cumsum(results_coins$tree_marg_gain[1:K]), col=col$coins)
legend('bottomright', legend=c('Optimized for tree', 'Optimized for coins'), col=c(col$tree, col$coins), lty=1)

# Compare under coins objective
plot(cumsum(results_coins$coins_marg_gain[1:K]), type='l', col=col$coins, ylab='Value under coins objective')
lines(cumsum(results_tree$coins_marg_gain[1:K]), col=col$tree)
lines(cumsum(results_chain$coins_marg_gain[1:K]), col=col$chain)
legend('bottomright', legend=c('Optimized for coins', 'Optimized for tree', 'Optimized for chain'),
       col=c(col$coins, col$tree, col$chain), lty=1)

# Unique sources among top k.
unique_at_k <- function(results, K) sapply(1:K, function(i) length(unique(results$src[1:i])))
K <- 1e4
uniq_chain <- unique_at_k(results_chain, K)
uniq_tree <- unique_at_k(results_tree, K)
uniq_coins <- unique_at_k(results_coins, K)
plot(uniq_chain, type='l', col=col$chain, panel.first=grid())
lines(uniq_tree, col=col$tree)
lines(uniq_coins, col=col$coins)
legend('bottomright', legend=c('Chain', 'Tree', 'Coins'), col=c(col$chain, col$tree, col$coins), lty=1)

avg_num_targets_per_source <- function(results, K)
  sapply(K, function(i) mean(tapply(results$tgt[1:i], results$src[1:i], length)))
K <- seq(1,1e4,100)
tgt_per_src_chain <- avg_num_targets_per_source(results_chain, K)
tgt_per_src_coins <- avg_num_targets_per_source(results_coins, K)
tgt_per_src_tree <- avg_num_targets_per_source(results_tree, K)

plot(K, tgt_per_src_coins, type='l', col=col$coins, panel.first=grid())
lines(K, tgt_per_src_tree, col=col$tree)
lines(K, tgt_per_src_chain, col=col$chain)
legend('bottomright', legend=c('Chain', 'Tree', 'Coins'), col=c(col$chain, col$tree, col$coins), lty=1)

avg_src_count_per_source <- function(results, K)
  sapply(K, function(i) mean(tapply(results$source_count_before[1:i], results$src[1:i], unique)))
K <- seq(1,1e4,100)
src_count_per_src_chain <- avg_src_count_per_source(results_chain, K)
src_count_per_src_coins <- avg_src_count_per_source(results_coins, K)
src_count_per_src_tree <- avg_src_count_per_source(results_tree, K)

plot(K, src_count_per_src_chain, type='l', col=col$coins, panel.first=grid())
lines(K, src_count_per_src_coins, col=col$tree)
lines(K, src_count_per_src_tree, col=col$chain)
legend('bottomright', legend=c('Chain', 'Tree', 'Coins'), col=c(col$chain, col$tree, col$coins), lty=1)

