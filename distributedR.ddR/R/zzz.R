.onAttach <- function(libname, pkgname) {
    ddR::register_driver(name = "DistributedR", initfunc = init_DR)
}
