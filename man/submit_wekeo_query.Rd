% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wekeo_api.R
\name{submit_wekeo_query}
\alias{submit_wekeo_query}
\title{Title}
\usage{
submit_wekeo_query(hda_list, query)
}
\arguments{
\item{hda_list}{dictionary initiated with the init function. Stores all information required to interact with HDA API}

\item{data}{a named list containing the dataset description}
}
\value{
Returns the hda_list including assigned job_id.
}
\description{
Submits a query job to the WEkEO database and receives a job ID in return. The Job ID is used to poll the status of the job and is required to then retrieve the results.
}
