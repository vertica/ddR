nInst = 2
errorRate <- function(a,b)
{
	if(is.dframe(a) | is.darray(a))
		a <- collect(a)
	if(is.dframe(b) | is.darray(b))
		b <- collect(b)
	if(is.data.frame(a))
		a <- a[,1]
	if(is.data.frame(b))
		b <- b[,1]
	if(is.factor(a))
		a <- as.character(a)
	if(is.factor(b))
		b <- as.character(b)
	sum(a!=b)/length(a)
}

rSquared <- function(a,b)
{
	if(is.dframe(a) | is.darray(a))
		a <- collect(a)
	if(is.dframe(b) | is.darray(b))
		b <- collect(b)
	if(is.data.frame(a))
		a <- a[,1]
	if(is.data.frame(b))
		b <- b[,1]
	mean_a = mean(a)
	a <- sapply(a,function(x,mean_a) (x-mean_a)^2, mean_a)
	b <- sapply(b,function(x,mean_a) (x-mean_a)^2, mean_a)
	sum(b)/sum(a)
}


########## Tests for formula interface ##########
# The input is combination of a formula and a data.frame or dframe

### Tests for data.frame ###
context("Checking the formula interface of drandomForest with data.frame")

test_that("the inputes are validated", {
    expect_error(drandomForest(Species ~ ., data=iris, nExecutor = nInst, ntree=-1))
    expect_error(drandomForest(Species ~ ., airquality, nExecutor = nInst))
    expect_error(drandomForest(Ozone ~ ., data=airquality, nExecutor=nInst)) # airquality contains NA
    expect_error(drandomForest(Ozone ~ ., data=airquality, nExecutor=nInst, na.action=na.pass))
})

test_that("the results are returned correctly", {
    hRF <- drandomForest(Species ~ ., data=iris, nExecutor = nInst)
    rRF <- randomForest(Species ~ ., data=iris)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$terms))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_identical(hRF$classes, rRF$classes)
    expect_is(hRF, "drandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, iris[-5])
    rpredicted <- predict(rRF, iris[-5])
    herrRate <- errorRate(iris[5], hpredicted)
    rerrRate <- errorRate(iris[5], rpredicted)

    expect_equivalent(herrRate, rerrRate)

    # complete mode
    expect_warning(hRFc <- drandomForest(Species ~ ., data=iris, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=iris[-5], ytest=iris[5]))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$votes))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))

    # handling missed data
    hRFm <- drandomForest(Ozone ~ ., data=airquality, nExecutor = nInst, na.action=na.omit, completeModel=TRUE)
    rRFm <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit)
    
    expect_identical(length(hRFm$y), length(rRFm$y))
})

### Tests for dframe ###
context("Checking the formula interface of drandomForest with dframe")
    Diris <- as.dframe(iris)
    Xiris <- as.dframe(iris[-5])
    Yiris <- as.dframe(iris[5])
    Dairquality <- as.dframe(airquality)

test_that("the inputes are validated", {
    expect_error(drandomForest(Species ~ ., data=Diris, nExecutor = nInst, ntree=-1))
    expect_error(drandomForest(Species ~ ., Dairquality, nExecutor = nInst))
    expect_error(drandomForest(Ozone ~ ., data=Dairquality, nExecutor=nInst))
    expect_error(drandomForest(Ozone ~ ., data=Dairquality, nExecutor=nInst, na.action=na.pass))
})

test_that("the results are returned correctly", {
    hRF <- drandomForest(Species ~ ., data=Diris, nExecutor = nInst)
    rRF <- randomForest(Species ~ ., data=iris)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$terms))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_identical(hRF$classes, rRF$classes)
    expect_is(hRF, "drandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, Xiris)
    rpredicted <- predict(rRF, iris[-5])
    herrRate <- errorRate(Yiris, hpredicted)
    rerrRate <- errorRate(iris[5], rpredicted)

    expect_equivalent(herrRate, rerrRate)

    # complete mode
    expect_warning(hRFc <- drandomForest(Species ~ ., data=Diris, nExecutor = nInst, completeModel=TRUE, proximity=TRUE))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$votes))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))

    # handling missed data
    hRFm <- drandomForest(Ozone ~ ., data=Dairquality, nExecutor = nInst, na.action=na.omit, completeModel=TRUE)
    rRFm <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit)
    
    expect_identical(length(hRFm$y), length(rRFm$y))
})

########## Tests for default interface ##########
# There are 2 main different scenarios for the inputs that are not available through formula: the input data is an ordinary R object, or it is darray

context("Checking the default interface of drandomForest with ordibary R objects")

cleandata <- na.omit(airquality)
x <- cleandata[-1]
y <- cleandata[1]

test_that("the inputes are validated", {
    expect_error(drandomForest(x=x, y=y, nExecutor = nInst, ntree=-1))
    expect_error(drandomForest(x=airquality[-1], y=airquality[1], nExecutor = nInst)) # airquality contains NA
    expect_error(drandomForest(x=x, y=y, nExecutor=100, ntree=50))
})

test_that("the results are returned correctly for regression", {
    hRF <- drandomForest(x=x, y=y, nExecutor = nInst, ntree=5000)
    rRF <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit, ntree=5000)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$mse))
    expect_false(is.null(hRF$rsq))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_is(hRF, "drandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, x)
    rpredicted <- predict(rRF, x)
    hrsq <- rSquared(y, hpredicted)
    rrsq <- rSquared(y, rpredicted)

    expect_true( abs(hrsq - rrsq) < 0.01)

    # complete mode
    expect_warning(hRFc <- drandomForest(x=x, y=y, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=x, ytest=y))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))

})

test_that("the results are returned correctly for unsupervised", {
    expect_warning(hRFc <- drandomForest(x=x, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, ntree=501, mtry=3))

    # availability of some outputs
    expect_false(is.null(hRFc$call))
    expect_false(is.null(hRFc$importance))
    expect_false(is.null(hRFc$proximity))

    # the value of some outputs
    expect_identical(hRFc$ntree, 501)
    expect_identical(hRFc$type, "unsupervised")
    expect_identical(hRFc$mtry, 3)
    expect_is(hRFc, "drandomForest")
    expect_is(hRFc, "randomForest")
})

context("Checking the default interface of drandomForest with darray")

X <- as.darray(data.matrix(x))
Y <- as.darray(data.matrix(y))

test_that("the inputes are validated", {
    expect_error(drandomForest(x=X, y=Y, nExecutor = nInst, ntree=-1))
    expect_error(drandomForest(x=X, y=Y, nExecutor=100, ntree=50))
})

test_that("the results are returned correctly", {
    hRF <- drandomForest(x=X, y=Y, nExecutor = nInst, ntree=5000)
    rRF <- randomForest(Ozone ~ ., data=airquality, na.action=na.omit, ntree=5000)

    # availability of some outputs
    expect_false(is.null(hRF$call))
    expect_false(is.null(hRF$importance))
    expect_false(is.null(hRF$mse))
    expect_false(is.null(hRF$rsq))

    # the value of some outputs
    expect_identical(hRF$ntree, rRF$ntree)
    expect_identical(hRF$type, rRF$type)
    expect_identical(hRF$mtry, rRF$mtry)
    expect_is(hRF, "drandomForest")
    expect_is(hRF, "randomForest")

    # prediction accuracy
    hpredicted <- predict(hRF, x)
    rpredicted <- predict(rRF, x)
    hrsq <- rSquared(y, hpredicted)
    rrsq <- rSquared(y, rpredicted)

    expect_true( abs(hrsq - rrsq) < 0.01)

    # complete mode
    expect_warning(hRFc <- drandomForest(x=x, y=y, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, xtest=x, ytest=y))
    expect_false(is.null(hRFc$predicted))
    expect_false(is.null(hRFc$oob.times))
    expect_false(is.null(hRFc$y))
    expect_false(is.null(hRFc$proximity))
    expect_false(is.null(hRFc$test))
})

test_that("the results are returned correctly for unsupervised", {
    expect_warning(hRFc <- drandomForest(x=X, nExecutor = nInst, completeModel=TRUE, proximity=TRUE, ntree=501, mtry=3))

    # availability of some outputs
    expect_false(is.null(hRFc$call))
    expect_false(is.null(hRFc$importance))
    expect_false(is.null(hRFc$proximity))

    # the value of some outputs
    expect_identical(hRFc$ntree, 501)
    expect_identical(hRFc$type, "unsupervised")
    expect_identical(hRFc$mtry, 3)
    expect_is(hRFc, "drandomForest")
    expect_is(hRFc, "randomForest")
})

context("Checking effectiveness of set.seed")

test_that("setting seed must make the output of the function deterministic", {

    generate_random_data <- function(x)
    {
	data.frame(matrix(sample.int(1e6, 50), nrow=10, ncol=5))
    }
    DFdata <- dmapply(generate_random_data, x = 1:nInst,
    	   output.type = "dframe", combine = "rbind", nparts = c(nInst,1))
    colnames(DFdata) <- c("X1", "X2", "X3", "X4", "X5")

    set.seed(100)
    model1 <- drandomForest(X1~., data=DFdata, nExecutor = nInst)

    set.seed(100)
    model2 <- drandomForest(X1~., data=DFdata, nExecutor = nInst)

    expect_true(all(model1$mse == model2$mse))
})
