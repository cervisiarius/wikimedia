vol <- function(Q) {
  v <- rep(0, length(Q))
  v[1] <- 1
  v[2] <- 1-Q[1]
  for (i in 3:length(Q)) {
    v[i] <- (1-Q[i-1])*v[i-1] + Q[i-2]*v[i-2]
  }
  v
}

phi <- function(Q) {
  sum(vol(Q)*Q)
}

# Probability of clicking at least one link.
p1 <- function(Q) {
  1 - prod(1-Q)
}

dA <- c()
dB <- c()
N <- 6
for (n in 1:1000) {
  By <- c(runif(N), 0, 0)
  
  xy <- sample(2:(N-2), 2)
  x <- xy[1]
  y <- xy[2]
  
  B <- By
  B[y] <- 0
  
  Ay <- By
  Ay[x] <- 0
  
  A <- Ay
  A[y] <- 0
  
  dA <- c(dA, p1(Ay)-p1(A))
  dB <- c(dB, p1(By)-p1(B))
  
#   dA <- c(dA, phi(Ay)-phi(A))
#   dB <- c(dB, phi(By)-phi(B))  
#   if (phi(Ay)-phi(A) < phi(By)-phi(B)) {
#     print(A)
#     print(Ay)
#     print(B) 
#     print(By)
#     print('-----------------')
#     break
#   }
}

plot(dA, dB, log='xy')
abline(0,1, col='red')

rbind(A,Ay)
rbind(vol(A),vol(Ay))
rbind(B,By)
rbind(vol(B),vol(By))

plot(vol(B), type='l', col='red')
lines(vol(By), lty=2, col='red')
lines(vol(A))
lines(vol(Ay), lty=2)

phisum <- function(Q) rev(cumsum(rev(vol(Q)*Q)))

plot(phisum(By), type='l', lty=2, col='red')
lines(phisum(B), col='red')
lines(phisum(Ay), lty=2)
lines(phisum(A))
legend('topright', legend=c('By','B','Ay','A'), col=c(2,2,1,1), lty=c(2,1,2,1))


Qxy <- c(rep(.5, 3), 0, 0)

Qx <- Qxy; Qx[3] <- 0
Qy <- Qxy; Qy[1] <- 0
Q <- Qxy; Q[3] <- Q[1] <- 0
plot(phisum(Qxy), type='l', lty=2, col='red')
lines(phisum(Qx), col='red')
lines(phisum(Qy), lty=2)
lines(phisum(Q))
legend('topright', legend=c('Qxy','Qx','Qy','Q'), col=c(2,2,1,1), lty=c(2,1,2,1))

plot(vol(Qxy), type='l', lty=2, col='red')
lines(vol(Qx), col='red')
lines(vol(Qy), lty=2)
lines(vol(Q))
legend('topright', legend=c('Qxy','Qx','Qy','Q'), col=c(2,2,1,1), lty=c(2,1,2,1))
























# Tabbed-browsing model.

f <- function(p_old, p_new) {
  1 - prod(1 - p_new / (sum(p_new) + sum(p_old)))
}

# Monotonicity.
summary(sapply(1:1000, function(i) {
  p_old <- runif(20)
  p_new <- runif(15)
  p <- runif(1)
  f(p_old, c(p_new, p)) - f(p_old, p_new)
  }))

# Submodularity.
summary(sapply(1:1000, function(i) {
  p_old <- runif(200)
  p_new <- runif(18)
  p <- runif(1)
  (f(p_old, c(p_new[-1], p)) - f(p_old, p_new[-1])) - (f(p_old, c(p_new, p)) - f(p_old, p_new))
}))



p_old <- c(.00001)
p_new <- .1
p <- .1
f(p_old, c(p_new, p)) - f(p_old, p_new)
