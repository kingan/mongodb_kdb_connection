#define KXVER 3
#include"k.h"
#include <bson.h>
#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>

K djnNewsStory(K x,K y){

        mongoc_client_t *client;
        mongoc_collection_t *collection;
        mongoc_cursor_t *cursor;
        const bson_t *doc;
        bson_t *query;
        char *str;

        mongoc_init ();

        K syms=ktn(0,0);

        client = mongoc_client_new ("mongodb://grv-infobright:27017/");
        collection = mongoc_client_get_collection (client, "djnNews", "djnNews_final");

        query = bson_new();

        BSON_APPEND_UTF8 (query,x->s,y->s);
        cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

        while (mongoc_cursor_next (cursor, &doc)) {
                str = bson_as_json (doc, NULL);
                K docu = kp(str);
                jk(&syms,docu);
                bson_free (str);
        }

        bson_destroy (query);
        mongoc_cursor_destroy (cursor);
        mongoc_collection_destroy (collection);
        mongoc_client_destroy (client);

        return syms;
}


K djnNewsStorynum(K x,K y){

        mongoc_client_t *client;
        mongoc_collection_t *collection;
        mongoc_cursor_t *cursor;
        const bson_t *doc;
        bson_t *query;
        char *str;

        mongoc_init ();

        K syms=ktn(0,0);

        client = mongoc_client_new ("mongodb://grv-infobright:27017/");
        collection = mongoc_client_get_collection (client, "djnNews", "djnNews_final");

        query = bson_new();

        bson_append_int64(query, x->s , -1 , y->j);
        cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);

        while (mongoc_cursor_next (cursor, &doc)) {
                str = bson_as_json (doc, NULL);
                K docu = kp(str);
                jk(&syms,docu);
                bson_free (str);
        }

        bson_destroy (query);
        mongoc_cursor_destroy (cursor);
        mongoc_collection_destroy (collection);
        mongoc_client_destroy (client);

        return syms;
}


K djnNewsIndexed(K x,K y){

        mongoc_client_t *client;
        mongoc_collection_t *collection;
        mongoc_cursor_t *cursor;
        const bson_t *doc;
        bson_t *query;
        char *str;

        mongoc_init ();

        K syms=ktn(0,0);

        client = mongoc_client_new ("mongodb://grv-infobright:27017/");
        collection = mongoc_client_get_collection (client, "djnNews", "djnNews_sample");

        query = BCON_NEW("$query","{","date",x->s,"$text","{","$search",y->s,"}","}");

        cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, NULL, NULL);


        while (mongoc_cursor_next (cursor, &doc)) {
                str = bson_as_json (doc, NULL);
                K docu = kp(str);
                jk(&syms,docu);
                bson_free (str);
        }

        bson_destroy (query);
        mongoc_cursor_destroy (cursor);
        mongoc_collection_destroy (collection);
        mongoc_client_destroy (client);

        return syms;
}


K djnNewsIndexed_specific_fields(K x,K y){

        mongoc_client_t *client;
        mongoc_collection_t *collection;
        mongoc_cursor_t *cursor;
        const bson_t *doc;
        bson_t *query;
        char *str;
        bson_t *fields;

        mongoc_init ();

        K syms=ktn(0,0);

        client = mongoc_client_new ("mongodb://grv-infobright:27017/");
        collection = mongoc_client_get_collection (client, "djnNews", "djnNews_sample");

        query = BCON_NEW("$query","{","date",x->s,"$text","{","$search",y->s,"}","}");
        //query = BCON_NEW("$query","{","date",x->s,"$text","{","$search",y->s,"}","}","score","{","$meta","{","textScore","}","}");
        fields = bson_new();
        BSON_APPEND_INT32(fields,"text",1);
        //BSON_APPEND_INT32(fields,"date",1);
        //BSON_APPEND_INT32(fields,"score",1);

        cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0, query, fields, NULL);


        while (mongoc_cursor_next (cursor, &doc)) {
                str = bson_as_json (doc, NULL);
                K docu = kp(str);
                jk(&syms,docu);
                bson_free (str);
        }

        bson_destroy (query);
        mongoc_cursor_destroy (cursor);
        mongoc_collection_destroy (collection);
        mongoc_client_destroy (client);

        return syms;
}


K aggregation(K x){

        mongoc_client_t *client;
        mongoc_collection_t *collection;
        mongoc_cursor_t *cursor;
        const bson_t *doc;
        bson_t *pipeline;
        char *str;
        bson_error_t error;
        mongoc_init ();

        K syms=ktn(0,0);

        client = mongoc_client_new ("mongodb://grv-infobright:27017");
        collection = mongoc_client_get_collection (client, "djnNews", "djnNews_sample");


        char *agg_str;
        strcpy (agg_str,"$");
        strcat (agg_str,x->s);


/*        pipeline = BCON_NEW( "pipeline", "[",
           "{", "$group", "{", "_id", "$date", "}" , "}",
           "{", "$project", "{", "_id", BCON_INT32(0), "date" , "$_id", "}", "}",
        "]");
*/

//db.djnNews_sample.aggregate([
//{$group:{_id:"$date"}},
//{$project:{"_id":1}}
//])

        pipeline = BCON_NEW( "pipeline", "[",
           "{", "$group", "{", "_id", agg_str, "}" , "}",
           "{", "$project", "{", "_id", BCON_INT32(0), x->s , "$_id", "}", "}",
           //"{", "$match", "{","date", BCON_UTF8("20140504"), "}", "}",
           //"{", "$limit", BCON_INT32(100),"}",
           //"{", "$unwind","$processed_text.tagged_text", "}",
           //"{", "$match", "{", "processed_text.tagged_text", "{", "$in", "[", BCON_UTF8 ("NNP"), "]" ,"}", "}", "}",
           //"{", "$project", "{","_id", BCON_INT32(1) , "processed_text.tagged_text", BCON_INT32(1), "}","}",
        "]");

        /*pipeline = BCON_NEW ("pipeline", "[",
           "{", "$unwind","$processed_text.tagged_text", "}",
           "{", "$match", "{", "$processed_text.tagged_text", "{", "$in", BCON_UTF8 ("NNP"), "}", "}", "}",
           "{", "$project", "{","_id", BCON_INT32(1) , "processed_text.tagged_text", BCON_INT32(1), "}","}",
           "{", "$limit", BCON_INT32(10),"}",
        "]");*/
		
		cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);

        while (mongoc_cursor_next (cursor, &doc)) {
                str = bson_as_json (doc, NULL);
                K docu = kp(str);
                jk(&syms,docu);
                bson_free (str);
        }

        bson_destroy (pipeline);
        mongoc_cursor_destroy (cursor);
        mongoc_collection_destroy (collection);
        mongoc_client_destroy (client);

        return syms;

        //if (mongoc_cursor_error (cursor, &error)) {
        //   fprintf (stderr, "Cursor Failure: %s\n", error.message);
        //}

}
