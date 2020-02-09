# Devoir_Soulémanou_NGBANGO
Mon travail se découpe en trois étapes:
1. Chargement des datasets
2. filtrage des données dans les datasets
3. jointure

### Explications
je filtre sur le "Code Etat établissment" le dataset des établissments afin de n'avoir  que des etéblissments ouverts. Puis je créé un nouveau dataset qui ne contient que les colonnes "code posta" et "nbre établissment par code postal". La dernière colonne correspond au nombre d'établiessement qui ont le même code postal.

Je filtre aussi le dataset des valeurs foncières afin de n'avoir que des données concernant des habitations de particulier.

Enfin, je fais une jointure de ces deux datasets en m'appuyant sur le code postal.

Vous trouverez plus de détails dans mon code.