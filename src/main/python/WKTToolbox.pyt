import arcpy


class Toolbox(object):
    def __init__(self):
        self.label = "Toolbox"
        self.alias = ""
        self.tools = [WKTTool]


class WKTTool(object):
    def __init__(self):
        self.label = "WKT Tool"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        paramName = arcpy.Parameter(
            name="in_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        paramName.value = "Z:\\Share\\stddist.txt"

        paramFC = arcpy.Parameter(
            name="stddist",
            displayName="stddist",
            direction="Output",
            datatype="DEFeatureClass",
            parameterType="Derived")

        return [paramName, paramFC]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        spref = arcpy.SpatialReference(102726)
        fc = "in_memory/stddist"
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arcpy.management.CreateFeatureclass("in_memory", "stddist", "POLYGON",
                                            spatial_reference=spref,
                                            has_m="DISABLED",
                                            has_z="DISABLED")
        arcpy.management.AddField(fc, "X_CENTER", "FLOAT")
        arcpy.management.AddField(fc, "Y_CENTER", "FLOAT")
        arcpy.management.AddField(fc, "STD_DIST", "FLOAT")
        cursor = arcpy.da.InsertCursor(fc, ['SHAPE@WKT', 'X_CENTER', 'Y_CENTER', 'STD_DIST'])
        f = open(parameters[0].value, "r")
        for line in f:
            t = line.rstrip('\n').split('\t')
            try:
                cursor.insertRow([t[0], t[1], t[2], t[3]])
            except:
                arcpy.AddMessage(t[0])
        f.close()
        del cursor
        parameters[1].value = fc
        return

