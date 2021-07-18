library(RCurl)
library(httr)
library(jsonlite)


#' generate_api_key
#'
#' Generates a base64-encoded api key based on the WEkEO user credentials username:password.
#'
#' @param username : WEkEO username
#' @param password : WEkEO password
#'
#' @return a string of the base64-encoded API key
#' @export
#'
#' @examples
generate_api_key = function(username, password){
  s = paste(username, password, sep = ':', collapse = ':')
  api_key = base64Encode(s, mode = 'character')
  return(api_key)
}

#' Title
#'
#' @param dataset_id
#' @param api_key
#' @param download_dir_path
#'
#' @return Returns the inititaed list
#' @export
#'
#' @examples
init = function(dataset_id, api_key, download_dir_path){
  hda_list = list()

  #Data broker address
  hda_list['broker_endpoint'] = 'https://wekeo-broker.apps.mercator.dpi.wekeo.eu/databroker'

  # Terms and conditions
  hda_list['acceptTandC_address'] = paste(hda_list['broker_endpoint'],
                                          '/termsaccepted/Copernicus_General_License', sep ='')
  # Access token address
  hda_list['accessToken_address'] = paste(hda_list['broker_endpoint'],
                                          '/gettoken', sep = '')

  # data request address
  hda_list['dataRequest_address'] = paste(hda_list['broker_endpoint'],
                                          '/datarequest', sep = '')

  #dataset id
  hda_list['dataset_id'] = dataset_id

  #API key
  hda_list['api_key'] = api_key

  #set HTTP success code
  hda_list['CONST_HTTP_SUCCESS_CODE'] = 200

  #download directory
  hda_list['downlaod_dir_path'] = download_dir_path


  return(hda_list)
}

#' Title
#'
#' Requests an access token to use the HDA API and stores it as a separate key in the dictionary
#'
#' @param hda_list : list initiated with the function init
#'
#' @return the list including the access token
#' @export
#'
#' @examples
get_access_token = function(hda_list){

  headers = list(Authorization = paste('Basic ', hda_list['api_key'], sep = ''))

  message('Getting access token. This token is valid for one hour only.')
  resp = GET(hda_list$accessToken_address,
             add_headers(.headers = unlist(headers)))

  # If the response code is 200 retrieve the token from the response
  if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
    access_token = content(resp)$access_token
    message('Success: Access-token is ', access_token)

    hda_list['access_token'] = access_token
    hda_list[['headers']] = list(Authorization = paste('Bearer ', hda_list$access_token, sep = ''),
                            Accept = 'application/json')
  }
  else{
    message('Error: Unexpected response')
    message(resp$headers)
    stop()
  }

  return(hda_list)
}


#' Title
#'
#' @param hda_list
#'
#' @return
#' @export
#'
#' @examples
acceptTandC = function(hda_list){
  msg1 = 'Accepting Terms and Conditions of Copernicus_General_licence'
  msg2 = 'Terms and Conditions already accepted'

  resp = GET(hda_list$acceptTandC_address,
             add_headers(.headers = unlist(hda_list$headers)))

  isTandCAccepted = content(resp)$accepted

  if(! isTandCAccepted){
    message(msg1)
    resp = PUT(hda_list$acceptTandC_address,
               add_headers(.headers = unlist(hda_list$headers)))
  }
  else{
    message( msg2 )
  }

  isTandCAccepted = content(resp)$accepted
  hda_list['isTandCAccepted'] = isTandCAccepted

  return(hda_list)
}



#' Title
#'
#' Submits a query job to the WEkEO database and receives a job ID in return. The Job ID is used to poll the status of the job and is required to then retrieve the results.
#'
#' @param hda_list dictionary initiated with the init function. Stores all information required to interact with HDA API
#' @param data a named list containing the dataset description
#'
#' @return Returns the hda_list including assigned job_id.
#' @export
#'
#' @examples
submit_wekeo_query = function(hda_list, query){

  resp = POST(hda_list$dataRequest_address,
             add_headers(.headers = unlist(hda_list$headers)),
             body = query, # Will auto-unbox. This part is somewhat fragile...
             encode = 'json'
             )

  if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
    job_id = content(resp)$jobId
    message('Query successfully submitted. Job ID is: ', job_id)
    hda_list['job_id'] = job_id
  }
  else{
    message('Error: Unexpected response', resp$headers)
  }

  #calls the polling function which checks the job on a periodic basis to see if it has finished. This effectively makes this a synchronous approach and therefore blocking. could be changed in future to not be blocking...
  get_query_status(hda_list)

  return(hda_list)
}

polling_time_control= function(t_step = 5, t_max = 60){

  t_wait = min(t_step, t_max)
  if(t_wait > 0){
    message('Next check in ', t_wait, ' seconds...')
    Sys.sleep(t_wait)
  }
}


#' Title
#'
#' @param hda_list
#'
#' @return
#' @export
#'
#' @examples
get_query_status = function(hda_list, t_step = 5, t_max = 60){

  #[ToDo] add functionality to trap a failed query... also timeouts...

  status = 'not started'
  count = 0

  while (status != 'completed') {

    # Avoid overloading server for long running queries.
    polling_time_control(t_step * count, t_max)

    resp = GET(paste(hda_list$dataRequest_address, '/status/', hda_list$job_id, sep = ''),
               add_headers(.headers = unlist(hda_list$headers)))

    if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
      status = content(resp)$status
      message(Sys.time(), ' Query status: ', status)
    } else{
      message('Error: unexpected response', resp$headers)
      stop()
    }

    count = count + 1
  }
}

#' get_query_results_list
#'
#' @param hda_list
#'
#' @return
#' @export
#'
#' @examples
get_query_results_list = function(hda_list){

  results_list = list()

  # Get the first page of results
  pages = 1000000 #some improbably large number
  page = 0

  # if there are more pages of results get those and add them to the results set

  while( pages > page){

    resp = GET( paste(hda_list$dataRequest_address, '/jobs/', hda_list$job_id, '/result', sep = ''),
                add_headers(.headers = unlist(hda_list$headers)),
                query = list(page = page))

    if(resp$status_code != hda_list$CONST_HTTP_SUCCESS_CODE){
      stop('request failed with response: ', content(resp)$detail)
    }

    results_list = append(results_list, content(resp)$content)

    #set the true value of the number of pages
    pages = content(resp)$pages

    #increment page
    page = page + 1
  }

  # add the result set to the dictionary
  hda_list[['results']] = results_list
  return(hda_list)
}

#' get_order_ids
#'
#' Gets the set of order ids corresponding to query results
#'
#' @param hda_list
#'
#' @return hda_list including order ids and the request staus of assigning the order Ids.
#' @export
#'
#' @examples
get_order_ids = function(hda_list){
  message('getting Order IDs.')

  #assume is fed a hda_list object
  results = hda_list$results

  # loop through the results which are to be downloaded and retrieve the corresponding order ID for each
  for (i in 1 : length(results) ) {

    data = list(jobId = hda_list$job_id,
                uri = results[[i]][['url']])


    resp = POST(paste(hda_list$broker_endpoint, '/dataorder', sep = ''),
                add_headers(.headers = unlist(hda_list$headers)),
                body = data, # Will auto-unbox. This part is somewhat fragile...
                encode = 'json'
    )

    if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
      message('Order id : ', content(resp)$orderId, ' submitted' )
      results[[i]][['order']] = content(resp)$orderId
      results[[i]][['orderStatus']] = 'submitted'
      results[[i]][['orderSubmitted']] = Sys.time()
    }
    else{
      message('Error: Unexpected response', fromJSON(content(resp, 'text'))$detail)
    }
  }

  hda_list[['results']] = results

  return(hda_list)
}



#' Title
#'
#' @param hda_list
#' @param file_extension Optional file extension to add to the downloaded files
#' @param user_filename
#'
#' @return
#' @export
#'
#' @examples
download_data = function(hda_list, file_extension = NULL){
  message('Downloading Files')

  # generate set of undownloaded files to monitor
  resultsIndices = 1 : length(hda_list$results)
  all_downloaded = F

  while (all_downloaded == F){

    downloadStates = rep.int(F, length(resultsIndices))
    s = 1
i = 1
    for(i in resultsIndices){

      # Check if order has completed. if so download it
      order_status = F

      resp = GET(paste(hda_list$broker_endpoint, '/dataorder/status/', hda_list$results[[i]]$order, sep = ''),
                  add_headers(.headers = unlist(hda_list$headers)))

      if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
        order_status = content(resp)$status
      }

      if(order_status == 'completed'){

        outDirectory = hda_list$downlaod_dir_path
        out_file_name = hda_list$results[[i]]$filename

        if(!is.null(file_extension))
          out_file_name = paste(out_file_name, file_extension, sep = '.')

        message('Downloading file: ', out_file_name)

        resp = GET(paste(hda_list$broker_endpoint, '/dataorder/download/', hda_list$results[[i]]$order, sep = ''),
                   write_disk( file.path(outDirectory, out_file_name), overwrite = T),
                   progress(),
                   add_headers(.headers = unlist(hda_list$headers)))

        if(resp$status_code == hda_list$CONST_HTTP_SUCCESS_CODE){
          message('Download Successful.')
          #remove from the list
          downloadStates[s] = T
        }

      }

      s = s + 1
    }

    resultsIndices = resultsIndices[! downloadStates ]

    all_downloaded = length(resultsIndices) == 0
  }

  return(hda_list)
}


