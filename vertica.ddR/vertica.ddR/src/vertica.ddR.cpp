#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <R.h>
#include <Rinternals.h>
#include <math.h>

inline void printHexByte(char byte, FILE * file);
inline void printHexDigit(char byte, FILE * file);

extern "C"
{
    SEXP write_csv_raw(SEXP obj, SEXP filename, 
		       SEXP R_delimiter, SEXP R_endofline)
{
    const char* delimiter = CHAR(STRING_ELT(R_delimiter,0));
    const char* endofline = CHAR(STRING_ELT(R_endofline,0));

    int num_digits = 4;
    int varbinary_size = 64000;
    int ncols = length(obj);
    if(ncols == 0)
	return R_NilValue;
    int nrows = length(VECTOR_ELT(obj,0));

    const char* filename_str = CHAR(STRING_ELT(filename,0));
    FILE * file = fopen(filename_str,"w");
    int col = 0;
    int * col_types = (int *) malloc(sizeof(int)*ncols);
    char ** format_strings = (char **) malloc(sizeof(char *)*ncols);
    int  memory_size = nrows;

    for( int col = 0; col < ncols; col++ ) 
    {
        switch( TYPEOF(VECTOR_ELT(obj,col)))
	{
	case REALSXP:
	{
	    col_types[col] = 1;
	    format_strings[col] = (char *) malloc(sizeof(char)*10);
	    memset(format_strings[col],0,10);
	    sprintf(format_strings[col],"%%0%d.0f",num_digits);
	    memory_size += num_digits * nrows;
	    break;
	}
	case INTSXP:
	{
	    col_types[col] = 2;
	    format_strings[col] = (char *) malloc(sizeof(char)*10);
	    memset(format_strings[col],0,10);
	    sprintf(format_strings[col],"%%0%dd",num_digits);
	    memory_size += num_digits * nrows;
	    break;
	}
	case VECSXP:
	{
	    col_types[col] = 3;
	    memory_size += 2*(varbinary_size+1) * nrows;
	    break;
	}
	};
    }    

    for(int row = 0; row < nrows; row++)
    {
	for(int col = 0; col < ncols; col++)
	{
	    char* col_string;
	    switch(col_types[col])
	    {
	    case 1:
	    {
		fprintf(file, format_strings[col], 
			REAL(VECTOR_ELT(obj,col))[row]);
		break;
	    }
	    case 2:
	    {
		fprintf(file, format_strings[col], 
			INTEGER(VECTOR_ELT(obj,col))[row]);
		break;
	    }
	    case 3:
	    {
		SEXP raw_bytes = VECTOR_ELT(VECTOR_ELT(obj,col),row);
		int len = length(raw_bytes);
		fprintf(file,"0x");
		int * ptr = (int *) RAW(raw_bytes);

		for(int i = 0; i < len && i < varbinary_size; i++)
		    printHexByte(RAW(raw_bytes)[i], file);


		break;
	    }
	    }
	    if(col < ncols -1)
		fprintf(file,"%s",delimiter);
	    else
		fprintf(file,"%s",endofline);
	}
    }

    fclose(file);
    free(col_types);

    return R_NilValue;
}
}

inline void printHexByte(char byte, FILE * file)
{
    char top = (byte >> 4) & 0xF;
    char bot = byte & 0xF;
    printHexDigit(top, file);
    printHexDigit(bot, file);
}

inline void printHexDigit(char byte, FILE * file)
{
    switch(byte)
    {
    case 0:
	fprintf(file,"0");
	break;
    case 1:
	fprintf(file,"1");
	break;
    case 2:
	fprintf(file,"2");
	break;
    case 3:
	fprintf(file,"3");
 	break;
    case 4:
	fprintf(file,"4");
	break;
    case 5:
	fprintf(file,"5");
	break;
    case 6:
	fprintf(file,"6");
	break;
    case 7:
	fprintf(file,"7");
	break;
    case 8:
	fprintf(file,"8");
	break;
    case 9:
	fprintf(file,"9");
	break;
    case 10:
	fprintf(file,"A");
 	break;
    case 11:
	fprintf(file,"B");
	break;
    case 12:
	fprintf(file,"C");
	break;
    case 13:
	fprintf(file,"D");
 	break;
    case 14:
	fprintf(file,"E");
 	break;
    case 15:
	fprintf(file,"F");
 	break;
    default:
 	break;
    }
}



extern "C"
{
    SEXP rapply_alt(SEXP obj, SEXP func, SEXP classes)
    {

	if(TYPEOF(obj) == VECSXP)
	    for(int i = 0; i < length(obj); i++)
	    {
		SEXP new_obj = rapply_alt(VECTOR_ELT(obj,i),func, classes);
		SET_VECTOR_ELT(obj,i, new_obj);
	    }
	SEXP obj_class;
	if(length(getAttrib(obj,install("class"))) == 0)
	{
	    if( TYPEOF(obj) == CLOSXP)
		obj_class = mkChar("function");
	    if( TYPEOF(obj) == INTSXP)
		obj_class = mkChar("integer");
	    if( TYPEOF(obj) == REALSXP)
		obj_class = mkChar("numeric");
	    if( TYPEOF(obj) == STRSXP)
		obj_class = mkChar("character");
	    if( TYPEOF(obj) == RAWSXP)
		obj_class = mkChar("raw");
	    else
		return obj;
	}
	else
	    obj_class = STRING_ELT(getAttrib(obj,install("class")),0);
	SEXP cur_class;
	for(int class_id = 0; class_id < length(classes); class_id++)
	{
	    cur_class = STRING_ELT(classes,class_id);
	    if(strcmp(CHAR(obj_class),CHAR(cur_class)) == 0)
	    {
		SEXP R_fcall;
		PROTECT(R_fcall = lang2(func, obj));
		R_fcall = eval(R_fcall, R_GlobalEnv );
		UNPROTECT(1);
		return R_fcall;
	    }
	}
	return obj;
    }
}
