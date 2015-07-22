.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/simtk/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG/simtk/', Sys.getenv('HOME'))

#### Results table

################### TODO: bootstrap CIs

pred_all <- read.table(sprintf('%s/p_eval_masterfile.tsv', DATADIR), header=TRUE, quote='', sep='\t')

pred_all$zero <- 0
pred <- pred_all[,c(11,8,7,14)]

nboot <- 1000
boot_ci <- function(data, f) {
  boot_samples <- apply(data[,-1], 2, function(x) boot(x, function(xx,j) f(data$p_groundtruth[j], xx[j]), nboot, ncpus=8))
  do.call(rbind, lapply(boot_samples, function(s) {
    b <- boot.ci(s, type='basic')
    c(b$basic[4], b$t0, b$basic[5])
  }))
}

# cols: lower 95% CI, estimated value, upper 95% CI
estim_MAE <- boot_ci(pred, function(pst, psthat) mean(abs(pst - psthat)))
estim_Pearson <- boot_ci(pred[,-4], function(pst, psthat) cor(pst, psthat, method='pearson'))
estim_Spearman <- boot_ci(pred[,-4], function(pst, psthat) cor(pst, psthat, method='spearman'))
estim_Pearson <- rbind(estim_Pearson, c(NA, NA, NA)); rownames(estim_Pearson)[nrow(estim_Pearson)] <- 'zero'
estim_Spearman <- rbind(estim_Spearman, c(NA, NA, NA)); rownames(estim_Spearman)[nrow(estim_Spearman)] <- 'zero'

# The mean between (y-lower) and (upper-y)
delta_MAE <- rowMeans(cbind(estim_MAE[,2] - estim_MAE[,1], estim_MAE[,3] - estim_MAE[,2]))
delta_Pearson <- rowMeans(cbind(estim_Pearson[,2] - estim_Pearson[,1], estim_Pearson[,3] - estim_Pearson[,2]))
delta_Spearman <- rowMeans(cbind(estim_Spearman[,2] - estim_Spearman[,1], estim_Spearman[,3] - estim_Spearman[,2]))

stringify <- function(estim, delta, digits) sprintf(sprintf('%%.%df (+-%%.%df)', digits, digits), estim[,2], delta)
tbl <- cbind(stringify(estim_MAE, delta_MAE, 3),
             stringify(estim_Pearson, delta_Pearson, 2),
             stringify(estim_Spearman, delta_Spearman, 2))
rownames(tbl) <- c('Path prop.', 'Mean baseln.', 'Zero baseln.')
colnames(tbl) <- c('MAE', 'Pearson', 'Spearman')

cat(gsub('\\(\\$\\\\pm\\$NA\\)', '',
         gsub('-', '$-$',
              gsub('\\+-', '$\\\\pm$', print(xtable(tbl), floating=FALSE)))))

#### Smoothed scatter plot of P_indir vs. p_st.

plot(pred$p_indirect, pred$p_groundtruth, log='xy')
plot(pred$baseline_mean, pred$p_groundtruth, log='xy')

ord <- order(pred$p_indirect)
x <- pred$p_indirect[ord]
y <- pred$p_groundtruth[ord]

plot(1:length(x), y, log='y')


if (save_plots) pdf(sprintf('%s/pindir_vs_pst.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
smooth <- function(y) ksmooth(1:length(y), y, bandwidth=10, kernel='normal')$y
plot(pred$p_indirect, pred$p_groundtruth, log='xy', col=c('#80808030'), xlim=c(2e-4, .28), ylim=c(2e-4, .28), bty='n',
     xlab='', ylab='', panel.first=abline(0, 1, col='gray'))
lines(x, smooth(y), lwd=1.5)
mtext('Path proportion', side=1, line=2.4)
mtext(expression(paste('Ground-truth clickthrough rate ', italic(p[st]))), side=2, line=2.4)
# plot(x, smooth(y), type='l', log='xy', xlim=c(2e-4, .2), ylim=c(2e-4, .2), bty='n',
#      xlab='Path proportion', ylab=expression(paste('Ground-truth clickthrough rate ', p[st])), panel.first=abline(0, 1, col='gray'))
if (save_plots) dev.off()

#################################################################
# Global optimization
#################################################################

DATADIR <- sprintf('%s/wikimedia/trunk/data/simtk/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG/simtk/', Sys.getenv('HOME'))

dice <- read.table(sprintf('%s/link_placement_results_DICE.tsv', DATADIR),
                   header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(dice) <- paste(dice$src, dice$tgt)

coins_page <- read.table(sprintf('%s/link_placement_results_COINS-PAGE.tsv', DATADIR),
                         header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(coins_page) <- paste(coins_page$src, coins_page$tgt)

coins_link <- read.table(sprintf('%s/link_placement_results_COINS-LINK.tsv', DATADIR),
                         header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(coins_link) <- paste(coins_link$src, coins_link$tgt)

col <- list(coins_page=rgb(.9,.6,0), dice=rgb(0,.45,.7), coins_link='black')

add_argmax_legend <- function() {
  legend('bottomright', col=c(col$coins_link, col$coins_page, col$dice), lty=c(2,1,1), bty='n',
         legend=c(expression(paste(italic(A), ' = argmax ', italic(f)[1], '(.)')),
                  expression(paste(italic(A), ' = argmax ', italic(f)[2], '(.)')),
                  expression(paste(italic(A), ' = argmax ', italic(f)[3], '(.)'))))
}

add_standard_legend <- function(pos='bottomright') {
  legend(pos, legend=c('Coins (link-centric)', 'Coins (page-centric)', 'Dice'),
         col=c(col$coins_link, col$coins_page, col$dice), lty=c(2,1,1), bty='n')
}

K <- 2000
num_src <- length(unique(dice$src))

# Summarize the sources that do not appear among the top K suggestions of the Dice model.
# Reason: never seen before.
summary(dice[dice$src %in% unique(dice$src)[!(unique(dice$src) %in% unique(dice$src[1:K]))],])

# Compare under dice objective
if (save_plots) pdf(sprintf('%s/objective_dice.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
# NB: The dice model used to be called "chain model".
plot(cumsum(dice$chain_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$dice,
     main='Return w.r.t. Dice', panel.first=abline(v=num_src, col='gray', lty=3))
lines(cumsum(coins_page$chain_marg_gain[1:K]), col=col$coins_page)
lines(cumsum(coins_link$chain_marg_gain[1:K]), col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[3], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Compare under coins (page-centric) objective
if (save_plots) pdf(sprintf('%s/objective_coins-page.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
# NB: The page-centric coins model used to be called "tree model".
plot(cumsum(coins_page$tree_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$coins_page,
     main='Return w.r.t. Coins (page)', panel.first=abline(v=num_src, col='gray', lty=3))
lines(cumsum(coins_link$tree_marg_gain[1:K]), col=col$coins_link, lty=2)
lines(cumsum(dice$tree_marg_gain[1:K]), col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[2], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Unique sources among top k.
unique_at_k <- function(results, K) sapply(1:K, function(i) length(unique(results$src[1:i])))
uniq_dice <- unique_at_k(dice, K)
uniq_coinspage <- unique_at_k(coins_page, K)
uniq_coinslink <- unique_at_k(coins_link, K)
if (save_plots) pdf(sprintf('%s/num_unique_sources.pdf', PLOTDIR), width=2, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(uniq_dice, type='l', xlab='', ylab='', bty='n', col=col$dice, ylim=c(0,800),
     main='Solution diversity', panel.first=abline(v=num_src, h=num_src, col='gray', lty=3))
lines(uniq_coinspage, col=col$coins_page)
lines(uniq_coinslink, col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Unique sources')), side=2, line=2.4)
add_standard_legend()
if (save_plots) dev.off()

# Number of targets per source.
avg_num_targets_per_source <- function(results, K)
  sapply(K, function(i) mean(tapply(results$tgt[1:i], results$src[1:i], length)))
Ks <- seq(1,K,1)
tgt_per_src_dice <- avg_num_targets_per_source(dice, Ks)
tgt_per_src_coinslink <- avg_num_targets_per_source(coins_link, Ks)
tgt_per_src_coinspage <- avg_num_targets_per_source(coins_page, Ks)
if (save_plots) pdf(sprintf('%s/num_targets_per_source.pdf', PLOTDIR), width=1.68, height=1.68, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(Ks, tgt_per_src_coinspage, type='l',  xlab='', ylab='', bty='n', col=col$coins_page,
     main='Solution concentration', ylim=c(1,5), panel.first=abline(v=num_src, col='gray', lty=3))
lines(Ks, tgt_per_src_coinslink, col=col$coins_link, lty=2)
lines(Ks, tgt_per_src_dice, col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Targets per source')), side=2, line=2.4)
add_standard_legend('topleft')
if (save_plots) dev.off()



# The following aren't used in the paper.

# Overlap of objectives
jaccard <- function(s1, s2) length(intersect(s1, s2)) / length(union(s1, s2))
Ks <- seq(1,K,1)
jacc_dice_coinslink <- sapply(Ks, function(i) jaccard(rownames(dice)[1:i], rownames(coins_link)[1:i]))
jacc_coinspage_coinslink <- sapply(Ks, function(i) jaccard(rownames(coins_page)[1:i], rownames(coins_link)[1:i]))
if (save_plots) pdf(sprintf('%s/jaccard_coefficient.pdf', PLOTDIR), width=2, height=1.5, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 0.8, 0.8))
plot(Ks, jacc_coinspage_coinslink, type='l', ylim=c(0,1), xlab='', ylab='', bty='n', col=col$coins_page,
     main='Solution overlap')
lines(Ks, jacc_dice_coinslink, col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Jaccard coefficient')), side=2, line=2.4)
legend('bottomright', legend=c('Coins (link) & Dice', 'Coins (link) & Coins (page)'),
       col=c(col$coins_page, col$dice), lty=1, bty='n')
if (save_plots) dev.off()

# Compare under link-centric coins objective
if (save_plots) pdf(sprintf('%s/objective_coins-link.pdf', PLOTDIR), width=2, height=1.5, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 1.2, 0.8))
plot(cumsum(coins_page$coins_marg_gain[1:K]), type='l', xlab='', ylab='', bty='n', col=col$coins_page, ylim=c(0,1000),
     main='Return w.r.t. Coins (link-centric)', panel.first=abline(v=num_src, col='gray'))
lines(cumsum(coins_link$coins_marg_gain[1:K]), col=col$coins_link, lty=2)
lines(cumsum(dice$coins_marg_gain[1:K]), col=col$dice)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Return ', italic(f)[1], '(', italic(A), ')')), side=2, line=2.4)
add_argmax_legend()
if (save_plots) dev.off()

# Number of pageviews per source.
Ks <- seq(1,K,1)
avg_src_count_per_source <- function(results, Ks) sapply(Ks, function(i) exp(median(results$source_count_before[1:i])))
src_count_per_src_dice <- avg_src_count_per_source(dice, Ks)
src_count_per_src_coinslink <- avg_src_count_per_source(coins_link, Ks)
src_count_per_src_coinspage <- avg_src_count_per_source(coins_page, Ks)
if (save_plots) pdf(sprintf('%s/num_pageviews_per_source.pdf', PLOTDIR), width=2, height=1.5, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 0.9, 0.8))
plot(Ks, src_count_per_src_coinspage, col=col$coins_page, type='l', xlab='', ylab='', bty='n', ylim=c(1,500), log='',
     main='Popularity of sources in solution')
lines(Ks, src_count_per_src_dice, col=col$dice)
lines(Ks, src_count_per_src_coinslink, col=col$coins_link, lty=2)
mtext(expression(paste('Size of solution ', italic(A))), side=1, line=2.4)
mtext(expression(paste('Pageviews per source')), side=2, line=2.4)
add_standard_legend()
if (save_plots) dev.off()
