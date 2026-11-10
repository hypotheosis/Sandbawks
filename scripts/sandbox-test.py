from e2b import Sandbox

# Boots a gVisor sandbox from the registered template.
sbx = Sandbox.create(template="busybox-template", timeout=300)
print("created:", sbx.sandbox_id)
#sbx = Sandbox.connect("sbx-1fa3f4b39f95")

# Exercises the envd exec proxy.
result = sbx.commands.run("echo hello-from-sandbawks")
print("stdout:", result.stdout.strip())

result = sbx.commands.run("ip a l")
print("stdout:", result.stdout.strip())

# Exercises the envd filesystem proxy.
sbx.files.write("/tmp/probe.txt", "ok")
print("readback:", sbx.files.read("/tmp/probe.txt"))

sbx.kill()
print("killed:", sbx.sandbox_id)

