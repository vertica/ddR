.onAttach <- function(libname, pkgname) {
  ddR::register_driver(name = "spark", initfunc = init_spark)
}
