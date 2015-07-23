.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/results/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG/global_opt/', Sys.getenv('HOME'))

dice <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_DICE.tsv.gz', DATADIR)),
                   header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(dice) <- paste(dice$src, dice$tgt)

coins_page <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_COINS-PAGE.tsv.gz', DATADIR)),
                         header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(coins_page) <- paste(coins_page$src, coins_page$tgt)

coins_link <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_COINS-LINK.tsv.gz', DATADIR)),
                         header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(coins_link) <- paste(coins_link$src, coins_link$tgt)

col <- list(coins_page=rgb(.9,.6,0), dice=rgb(0,.45,.7), coins_link='black')

add_argmax_legend <- function() {
  legend('bottomright', col=c(col$coins_link, col$coins_page, col$dice), lty=c(2,1,1), bty='n',
         legend=c(expression(paste(italic(A), ' = argmax ', italic(f)[1], '(.)')),
                  expression(paste(italic(A), ' = argmax ', italic(f)[2], '(.)')),
                  expression(paste(italic(A), ' = argmax ', italic(f)[3], '(.)'))))
}

add_standard_legend <- function() {
  legend('bottomright', legend=c('Coins (link-centric)', 'Coins (page-centric)', 'Dice'),
         col=c(col$coins_link, col$coins_page, col$dice), lty=c(2,1,1), bty='n')
}

# Overlap of objectives
jaccard <- function(s1, s2) length(intersect(s1, s2)) / length(union(s1, s2))
K <- seq(1,1e4,10)
jacc_dice_coinslink <- sapply(K, function(i) jaccard(rownames(dice)[1:i], rownames(coins_link)[1:i]))
jacc_coinspage_coinslink <- sapply(K, function(i) jaccard(rownames(coins_page)[1:i], rownames(coins_link)[1:i]))
if (save_plots) pdf(sprintf('%s/jaccard_coefficient.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(K, jacc_coinspage_coinslink, type='l', ylim=c(0.2,1), xlab='', ylab='', bty='n', col=col$coins_page,
     main='Solution overlap')
lines(K, jacc_dice_coinslink, col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Jaccard coefficient')), side=2, line=2.4)
legend('bottomright', legend=c('Coins (link) & Coins (page)', 'Coins (link) & Dice'),
       col=c(col$coins_page, col$dice), lty=1, bty='n')
if (save_plots) dev.off()

# Compare under dice objective
K <- 1e4
if (save_plots) pdf(sprintf('%s/objective_dice.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
# NB: The dice model used to be called "chain model".
plot(cumsum(dice$chain_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$dice,
     main='Return w.r.t. Dice')
lines(cumsum(coins_page$chain_marg_gain[1:K]), col=col$coins_page)
lines(cumsum(coins_link$chain_marg_gain[1:K]), col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[3], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Compare under coins (page-centric) objective
K <- 1e4
if (save_plots) pdf(sprintf('%s/objective_coins-page.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
# NB: The page-centric coins model used to be called "tree model".
plot(cumsum(coins_page$tree_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$coins_page,
     main='Return w.r.t. Coins (page)')
lines(cumsum(coins_link$tree_marg_gain[1:K]), col=col$coins_link, lty=2)
lines(cumsum(dice$tree_marg_gain[1:K]), col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[2], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Compare under link-centric coins objective
K <- 1e4
if (save_plots) pdf(sprintf('%s/objective_coins-link.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(cumsum(coins_page$coins_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$coins_page,
     main='Return w.r.t. Coins (link)')
lines(cumsum(coins_link$coins_marg_gain[1:K]), col=col$coins_link, lty=2)
lines(cumsum(dice$coins_marg_gain[1:K]), col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[1], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Unique sources among top k.
unique_at_k <- function(results, K) sapply(1:K, function(i) length(unique(results$src[1:i])))
K <- 1e4
uniq_dice <- unique_at_k(dice, K)
uniq_coinspage <- unique_at_k(coins_page, K)
uniq_coinslink <- unique_at_k(coins_link, K)
if (save_plots) pdf(sprintf('%s/num_unique_sources.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(uniq_dice, type='l', xlab='', ylab='', bty='n', col=col$dice,
     main='Solution diversity')
lines(uniq_coinspage, col=col$coins_page)
lines(uniq_coinslink, col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Unique sources')), side=2, line=2.4)
add_standard_legend()
if (save_plots) dev.off()

# Number of targets per source.
avg_num_targets_per_source <- function(results, K)
  sapply(K, function(i) mean(tapply(results$tgt[1:i], results$src[1:i], length)))
K <- seq(1,1e4,10)
tgt_per_src_dice <- avg_num_targets_per_source(dice, K)
tgt_per_src_coinslink <- avg_num_targets_per_source(coins_link, K)
tgt_per_src_coinspage <- avg_num_targets_per_source(coins_page, K)
if (save_plots) pdf(sprintf('%s/num_targets_per_source.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(K, tgt_per_src_coinspage, type='l',  xlab='', ylab='', bty='n', col=col$coins_page,
     main='Solution concentration', ylim=c(1,3))
lines(K, tgt_per_src_coinslink, col=col$coins_link, lty=2)
lines(K, tgt_per_src_dice, col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Targets per source')), side=2, line=2.4)
add_standard_legend()
if (save_plots) dev.off()

# Number of pageviews per source.
avg_src_count_per_source <- function(results, K) sapply(K, function(i) exp(median(results$source_transition_count_before[1:i])))
K <- seq(1,1e4,100)
src_count_per_src_dice <- avg_src_count_per_source(dice, K)
src_count_per_src_coinslink <- avg_src_count_per_source(coins_link, K)
src_count_per_src_coinspage <- avg_src_count_per_source(coins_page, K)
if (save_plots) pdf(sprintf('%s/num_pageviews_per_source.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(K, src_count_per_src_coinspage, col=col$coins_page, type='l', xlab='', ylab='', bty='n', ylim=c(0,5000),
     main='Popularity of sources in soln.')
lines(K, src_count_per_src_dice, col=col$dice)
lines(K, src_count_per_src_coinslink, col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Pageviews per source')), side=2, line=2.4)
add_standard_legend()
if (save_plots) dev.off()
