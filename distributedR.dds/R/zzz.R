###################################################################
# Copyright 2015 Hewlett-Packard Development Company, L.P.
# This program is free software; you can redistribute it 
# and/or modify it under the terms of the GNU General Public 
# License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
# General Public License for more details.

# You should have received a copy of the GNU General Public License 
# along with this program; if not, write to the Free Software 
# Foundation, Inc., 59 Temple Place, Suite 330, 
# Boston, MA 02111-1307 USA.
###################################################################

# Do not allow DistR to overwrite dds definitions if this package is loaded after dds
.onAttach <- function(libname, pkgname) {
  assign("dlist", dds::dlist, envir=globalenv())
  assign("darray", dds::darray, envir=globalenv())
  assign("dframe", dds::dframe, envir=globalenv())
  assign("is.darray", dds::is.darray, envir=globalenv())
  assign("is.dframe", dds::is.dframe, envir=globalenv())
  assign("is.dlist", dds::is.dlist, envir=globalenv())
  assign("as.dlist", dds::as.dlist, envir=globalenv())
  assign("as.darray", dds::as.darray, envir=globalenv())
  assign("as.dframe", dds::as.dframe, envir=globalenv())
}
