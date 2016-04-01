        upload_consumption_dataframe(consumption_df,
                          self.settings['datastore_url'],
                          self.settings['datastore_token'],
                          self.settings['project_owner'],
                          verbose=True)
