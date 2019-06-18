# outline

The tables we are after are [here](http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=99480015&annee=2018&mois=12&xt=prt) (example URL with precipitation data from station 9948):
```
http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=99480015&annee=2018&mois=12&xt=prt
```
And this example table looks like this:
```html
<table summary="" cellspacing="2" cellpadding="2" border="0" width="100%">
<tbody><tr align="center">
<th width="9%"></th><th class="statmois" width="3%">1</th><th class="statmois" width="3%">2</th><th class="statmois" width="3%">3</th>
</tr>
<tr align="right">
<td align="center" class="stathead">1</td><td class="">0.00</td><td class="">0.00</td>
</tr>
<tr align="right" bgcolor="#e0ffff">
<td align="center" class="stathead">2</td><td class="">0.00</td><td class="">0.00</td>
</tr>
</tbody></table>

```

<table summary="" cellspacing="2" cellpadding="2" border="0" width="100%">
<tbody><tr align="center">
<th width="9%"></th><th class="statmois" width="3%">1</th><th class="statmois" width="3%">2</th><th class="statmois" width="3%">3</th>
</tr>
<tr align="right">
<td align="center" class="stathead">1</td><td class="">0.00</td><td class="">0.00</td>
</tr>
<tr align="right" bgcolor="#e0ffff">
<td align="center" class="stathead">2</td><td class="">0.00</td><td class="">0.00</td>
</tr>
</tbody></table>

This is just a fraction of the full table. The whole thing is 31 days wide and stands 24 hours tall. Each cell holds a measurement for that day and that hour. 

DONE: Our challenge is to unravel these tables, month by month, station by station. 

DONE: Next we need to write these data into a chronological database.

### Additional challenges
- DOING: Finding the correct starting/ending points in time for this station;
    - TODO: This needs to be dynamic: depending on data already stored in the chronological database, we might want to harvest only the newest entries. So we need to look up in the DB which timesteps are already filled with data.
    - DONE: Each station has its own availablity of data. The table page indicates the period of availablility.
        ```html
        <tbody><tr align="left">
        <td nowrap="" width="20%">Station : BASTOGNE            </td><td nowrap="" width="20%">Rivière : SURE                </td><td nowrap="" width="25%">Période : 01/2002 - 06/2019</td><td align="right" nowrap="" width="35%"></td>
        </tr>
        </tbody>
        ```
- DONE: Constructing the URL with all the correct parameters. Maybe (TODO?) use URLLIB this time?
